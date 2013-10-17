/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.summingbird.scalding


import com.twitter.algebird.bijection.BijectedSemigroup
import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.algebird.monad.{StateWithError, Reader}
import com.twitter.bijection.{ Bijection, ImplicitBijection }
import com.twitter.scalding.{Dsl, Mode, TypedPipe, IterableSource, TupleSetter, TupleConverter}
import com.twitter.scalding.typed.Grouped
import com.twitter.summingbird._
import com.twitter.summingbird.option._
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp}
import cascading.flow.FlowDef

trait ScaldingStore[K, V] extends java.io.Serializable {
  /**
    * Accepts deltas along with their timestamps, returns triples of
    * (time, K, V(aggregated up to the time)).
    *
    * Same return as lookup on a ScaldingService.
    */
  def merge(delta: PipeFactory[(K, V)],
    sg: Semigroup[V],
    commutativity: Commutativity,
    reducers: Int): PipeFactory[(K, (Option[V], V))]
}

trait BatchedScaldingStore[K, V] extends ScaldingStore[K, V] { self =>
  /** The batcher for this store */
  def batcher: Batcher

  implicit def ordering: Ordering[K]

  /**
    * Override select if you don't want to materialize every
    * batch. Note that select MUST return a list containing the final
    * batch in the supplied list; otherwise data would be lost.
    */
  def select(b: List[BatchID]): List[BatchID] = b

  /**
   * For (firstNonZero - 1) we read empty. For all before we error on read. For all later, we proxy
   * On write, we throw if batchID is less than firstNonZero
   */
  def withInitialBatch(firstNonZero: BatchID): BatchedScaldingStore[K, V] =
    new InitialBatchedStore(firstNonZero, self)

  /** Get the most recent last batch and the ID (strictly less than the input ID)
   * The "Last" is the stream with only the newest value for each key, within the batch
   * combining the last from batchID and the deltas from batchID.next you get the stream
   * for batchID.next
   */
  def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])]

  /** Record a computed batch of code */
  def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit

  /** The writeLast method as a FlowProducer */
  private def writeFlow(batches: List[BatchID], lastVals: TypedPipe[(BatchID, (K, V))]): FlowProducer[Unit] =
    Reader[FlowInput, Unit] { case (flow, mode) =>
      // make sure we checkpoint to disk to avoid double computation:
      val checked = if(batches.size > 1) lastVals.forceToDisk else lastVals
      batches.foreach { batchID =>
        val thisBatch = checked.filter { case (b, _) => b == batchID }
        writeLast(batchID, thisBatch.values)(flow, mode)
      }
    }

  /** we are guaranteed to have sufficient input and deltas to cover these batches
   * and that the batches are given in order
   */
  private def mergeBatched(inBatch: BatchID,
    input: FlowProducer[TypedPipe[(K,V)]],
    batchIntr: Interval[BatchID],
    deltas: FlowToPipe[(K,V)],
    commutativity: Commutativity,
    reducers: Int)(implicit sg: Semigroup[V]): FlowToPipe[(K,(Option[V], V))] = {

    val batches = BatchID.toIterable(batchIntr).toList
    val finalBatch = batches.last // batches won't be empty.
    val filteredBatches = select(batches).sorted
    assert(filteredBatches.contains(finalBatch), "select must not remove the final batch.")

    import IteratorSums._ // get the groupedSum, partials function

    def prepareOld(old: TypedPipe[(K, V)]): TypedPipe[(K, (BatchID, (Timestamp, V)))] =
      old.map { case (k, v) => (k, (inBatch, (Timestamp.Min, v))) }

    val capturedBatcher = batcher //avoid a closure on the whole store
    def prepareDeltas(ins: TypedPipe[(Long, (K, V))]): TypedPipe[(K, (BatchID, (Timestamp, V)))] = {
      val inits = ins.map { case (t, (k, v)) =>
        val ts = Timestamp(t)
        val batch = capturedBatcher.batchOf(ts)
        ((k, batch), (ts, v))
      }
      (commutativity match {
        case Commutative => Scalding.mapsideReduce(inits)
        case NonCommutative => inits
        })
        .map { case ((k, batch), (ts, v)) => (k, (batch, (ts, v))) }
    }

    /** Produce a merged stream such that each BatchID, Key pair appears only one time.
     */
    def mergeAll(all: TypedPipe[(K, (BatchID, (Timestamp, V)))]):
      TypedPipe[(K, (BatchID, (Option[Option[(Timestamp, V)]], Option[(Timestamp, V)])))] =
      all.group
        .withReducers(reducers)
        .sortBy { case (_, (t, _)) => t } //we are sorted on time, therefore BatchID
        .mapValueStream { it =>
          // each BatchID appears at most once, so it fits in RAM
          val batched: Map[BatchID, (Timestamp, V)] = groupedSum(it).toMap
          partials((inBatch :: batches).iterator.map { bid => (bid,  batched.get(bid)) })
        }
        .toTypedPipe

    /**
     * There is no flatten on Option, this adds it
     */
    def flatOpt[T](optopt: Option[Option[T]]): Option[T] = optopt.flatMap(identity)

    // This builds the format we write to disk, which is the total sum
    def toLastFormat(res: TypedPipe[(K, (BatchID, (Option[Option[(Timestamp, V)]], Option[(Timestamp, V)])))]):
      TypedPipe[(BatchID, (K, V))] =
        res.flatMap { case (k, (batchid, (prev, v))) =>
          val totalSum = Semigroup.plus[Option[(Timestamp, V)]](flatOpt(prev), v)
          totalSum.map { case (_, sumv) => (batchid, (k, sumv)) }
        }

    // This builds the format we send to consumer nodes
    def toOutputFormat(res: TypedPipe[(K, (BatchID, (Option[Option[(Timestamp, V)]], Option[(Timestamp, V)])))]):
      TypedPipe[(Long, (K, (Option[V], V)))] =
        res.flatMap { case (k, (batchid, (optopt, opt))) =>
          opt.map { case (ts, v) =>
            val prev = flatOpt(optopt).map(_._2)
            (ts.milliSinceEpoch, (k, (prev, v)))
          }
        }

    // Now in the flow-producer monad; do it:
    for {
      pipeInput <- input
      pipeDeltas <- deltas
      // fork below so scalding can make sure not to do the operation twice
      merged = mergeAll(prepareOld(pipeInput) ++ prepareDeltas(pipeDeltas)).fork
      lastOut = toLastFormat(merged)
      _ <- writeFlow(filteredBatches, lastOut)
    } yield toOutputFormat(merged)
  }

  /** instances of this trait MAY NOT change the logic here. This always follows the rule
   * that we look for existing data (avoiding reading deltas in that case), then we fall
   * back to the last checkpointed output by calling readLast. In that case, we compute the
   * results by rolling forward
   */
  final override def merge(delta: PipeFactory[(K, V)],
    sg: Semigroup[V],
    commutativity: Commutativity,
    reducers: Int): PipeFactory[(K, (Option[V], V))] = StateWithError({ in: FactoryInput =>
      val (timeSpan, mode) = in
      // This object combines some common scalding batching operations:
      val batchOps = new BatchedOperations(batcher)

      (batchOps.coverIt(timeSpan).toList match {
        case Nil => Left(List("Timespan is covered by Nil: %s batcher: %s".format(timeSpan, batcher)))
        case list => Right((list.min, list.max))
      })
      .right
      .flatMap { case (firstNewBatch, lastNewBatch) =>
        readLast(firstNewBatch, mode)
          .right
          .flatMap { case (actualLast, input) =>
            val firstDeltaBatch = actualLast.next
            // Compute the times we need to read of the deltas
            val deltaBatches = Interval.leftClosedRightOpen(firstDeltaBatch, lastNewBatch.next)
            batchOps.readBatched(deltaBatches, mode, delta)
              .right
              .flatMap { case (batchesWeCanBuild, deltaFlow2Pipe) =>

                // Check that deltas needed can actually be loaded going back to the first new batch
                if(!batchesWeCanBuild.contains(firstDeltaBatch)) {
                  Left(List("Cannot load an entire initial batch: " + firstDeltaBatch.toString
                    + " of deltas at: " + this.toString + " only: " + batchesWeCanBuild.toString))
                }
                else {
                  val merged = mergeBatched(actualLast, input, batchesWeCanBuild,
                    deltaFlow2Pipe, commutativity, reducers)(sg)
                  val available = batchOps.intersect(batchesWeCanBuild, timeSpan)
                  val filtered = Scalding.limitTimes(available, merged)
                  Right(((available, mode), filtered))
                }
              }
          }
        }
    })
}


/**
 * For (firstNonZero - 1) we read empty. For all before we error on read. For all later, we proxy
 * On write, we throw if batchID is less than firstNonZero
 */
class InitialBatchedStore[K,V](val firstNonZero: BatchID, val proxy: BatchedScaldingStore[K, V])
  extends BatchedScaldingStore[K, V] {

  def batcher = proxy.batcher
  def ordering = proxy.ordering
  def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode) =
    if (batchID >= firstNonZero) proxy.writeLast(batchID, lastVals)
    else sys.error("Earliest batch set at :" + firstNonZero + " but tried to write: " + batchID)

  // Here is where we switch:
  def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    if (exclusiveUB > firstNonZero) proxy.readLast(exclusiveUB, mode)
    else if (exclusiveUB == firstNonZero) Right((firstNonZero.prev, Scalding.emptyFlowProducer[(K,V)]))
    else Left(List("Earliest batch set at :" + firstNonZero + " but tried to read: " + exclusiveUB))
  }
}
