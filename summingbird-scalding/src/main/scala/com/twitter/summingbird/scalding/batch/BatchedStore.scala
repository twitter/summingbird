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

package com.twitter.summingbird.scalding.batch

import com.twitter.algebird.bijection.BijectedSemigroup
import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.algebird.monad.{StateWithError, Reader}
import com.twitter.bijection.{ Bijection, ImplicitBijection }
import com.twitter.scalding.{Dsl, Mode, TypedPipe, IterableSource, MapsideReduce, TupleSetter, TupleConverter}
import com.twitter.scalding.typed.Grouped
import com.twitter.summingbird.scalding._
import com.twitter.summingbird.scalding
import com.twitter.summingbird._
import com.twitter.summingbird.option._
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp, IteratorSums, PrunedSpace}
import cascading.flow.FlowDef

import org.slf4j.LoggerFactory

trait BatchedStore[K, V] extends scalding.Store[K, V] { self =>
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
   * Override this to set up store pruning, by default, no (key,value) pairs
   * are pruned. This is a house keeping function to permanently remove entries
   * matching a criteria.
   */
  def pruning: PrunedSpace[(K, V)] = PrunedSpace.neverPruned

  /**
   * Override this to control when keys are frozen. This allows
   * us to avoid sorting and shuffling keys that are not updated.
   */
  def boundedKeySpace: TimeBoundedKeySpace[K] = TimeBoundedKeySpace.neverFrozen

  /**
   * For (firstNonZero - 1) we read empty. For all before we error on read. For all later, we proxy
   * On write, we throw if batchID is less than firstNonZero
   */
  def withInitialBatch(firstNonZero: BatchID): BatchedStore[K, V] =
    new scalding.store.InitialBatchedStore(firstNonZero, self)

  /** Get the most recent last batch and the ID (strictly less than the input ID)
   * The "Last" is the stream with only the newest value for each key, within the batch
   * combining the last from batchID and the deltas from batchID.next you get the stream
   * for batchID.next
   */
  def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])]

  /** Record a computed batch of code */
  def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit

  @transient private val logger = LoggerFactory.getLogger(classOf[BatchedStore[_,_]])

  /** The writeLast method as a FlowProducer */
  private def writeFlow(batches: List[BatchID], lastVals: TypedPipe[(BatchID, (K, V))]): FlowProducer[Unit] = {
    logger.info("writing batches: {}", batches)
    Reader[FlowInput, Unit] { case (flow, mode) =>
      // make sure we checkpoint to disk to avoid double computation:
      val checked = if(batches.size > 1) lastVals.forceToDisk else lastVals
      batches.foreach { batchID =>
        val thisBatch = checked.filter { case (b, kv) =>
             (b == batchID) && !pruning.prune(kv, batcher.latestTimeOf(b))
        }
        writeLast(batchID, thisBatch.values)(flow, mode)
      }
    }
  }

  protected def sumByBatches[K1,V:Semigroup](ins: TypedPipe[(Timestamp, (K1, V))],
    capturedBatcher: Batcher,
    commutativity: Commutativity): TypedPipe[((K1, BatchID), (Timestamp, V))] = {
      implicit val timeValueSemigroup: Semigroup[(Timestamp, V)] =
        IteratorSums.optimizedPairSemigroup[Timestamp, V](1000)

      val inits = ins.map { case (t, (k, v)) =>
        val batch = capturedBatcher.batchOf(t)
        ((k, batch), (t, v))
      }
      (commutativity match {
        case Commutative => Store.mapsideReduce(inits)
        case NonCommutative => inits
        })
    }

  /** For each batch, collect up values with the same key on mapside
   * before the keys are expanded.
   */
  override def partialMerge[K1](delta: PipeFactory[(K1, V)],
    sg: Semigroup[V],
    commutativity: Commutativity): PipeFactory[(K1, V)] = {
    logger.info("executing partial merge")
    implicit val semi = sg
    val capturedBatcher = batcher
    commutativity match {
      case Commutative => delta.map { flow =>
        flow.map { typedP =>
          sumByBatches(typedP, capturedBatcher, Commutative)
            .map { case ((k, _), (ts, v)) => (ts, (k, v)) }
        }
      }
      case NonCommutative => delta
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

    logger.info("Previous written batch: {}, computing: {}", inBatch.asInstanceOf[Any], batches)

    def prepareOld(old: TypedPipe[(K, V)]): TypedPipe[(K, (BatchID, (Timestamp, V)))] =
      old.map { case (k, v) => (k, (inBatch, (Timestamp.Min, v))) }

    val capturedBatcher = batcher //avoid a closure on the whole store

    def prepareDeltas(ins: TypedPipe[(Timestamp, (K, V))]): TypedPipe[(K, (BatchID, (Timestamp, V)))] =
      sumByBatches(ins, capturedBatcher, commutativity)
        .map { case ((k, batch), (ts, v)) => (k, (batch, (ts, v))) }

    /** Produce a merged stream such that each BatchID, Key pair appears only one time.
     */
    def mergeAll(all: TypedPipe[(K, (BatchID, (Timestamp, V)))]):
      TypedPipe[(K, (BatchID, (Option[Option[(Timestamp, V)]], Option[(Timestamp, V)])))] = {

      // Make sure to use sumOption on V
      implicit val timeValueSemigroup: Semigroup[(Timestamp, V)] =
        IteratorSums.optimizedPairSemigroup[Timestamp, V](1000)

      val grouped = all.group.withReducers(reducers)

      // We need the tuples to be sorted by batch ID to compute partials, and by time if the
      // monoid is not commutative. Although sorting by time is adequate for both, sorting
      // by batch is more efficient because the reducers' input is almost sorted.
      val sorted = commutativity match {
        case NonCommutative => grouped.sortBy { case (_, (t, _)) => t }
        case Commutative => grouped.sortBy { case (b, (_, _)) => b }
      }

      sorted
        .mapValueStream { it =>
          // each BatchID appears at most once, so it fits in RAM
          val batched: Map[BatchID, (Timestamp, V)] = groupedSum(it).toMap
          partials((inBatch :: batches).iterator.map { bid => (bid,  batched.get(bid)) })
        }
        .toTypedPipe
      }

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
      TypedPipe[(Timestamp, (K, (Option[V], V)))] =
        res.flatMap { case (k, (batchid, (optopt, opt))) =>
          opt.map { case (ts, v) =>
            val prev = flatOpt(optopt).map(_._2)
            (ts, (k, (prev, v)))
          }
        }

    // Avoid closures where easy
    val thisTimeInterval = capturedBatcher.toTimestamp(batchIntr)
    val capturedKeyCheck = boundedKeySpace

    def getFrozenKeys(p: TypedPipe[(K,V)]): TypedPipe[(BatchID, (K, V))] =
      p.filter { case (k, _) => capturedKeyCheck.isFrozen(k, thisTimeInterval) }
        .flatMap { kv => filteredBatches.map { (_, kv) } }

    def getLiquidKeys(p: TypedPipe[(K,V)]): TypedPipe[(K, V)] =
      p.filter { case (k, _) => !capturedKeyCheck.isFrozen(k, thisTimeInterval) }

    def assertDeltasAreLiquid(p: TypedPipe[(Long, (K, V))]): TypedPipe[(Long, (K, V))] =
      p.map { tkv =>
        assert(!capturedKeyCheck.isFrozen(tkv._2._1, thisTimeInterval), "Frozen key in deltas: " + tkv)
        tkv
      }

    // Now in the flow-producer monad; do it:
    for {
      pipeInput <- input
      frozen = getFrozenKeys(pipeInput)
      liquid = getLiquidKeys(pipeInput)
      ds <- deltas
      liquidDeltas = assertDeltasAreLiquid(ds)
      // fork below so scalding can make sure not to do the operation twice
      merged = mergeAll(prepareOld(liquid) ++ prepareDeltas(liquidDeltas)).fork
      lastOut = toLastFormat(merged) ++ frozen
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
