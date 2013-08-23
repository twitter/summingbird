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

import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.algebird.monad.{StateWithError, Reader}
import com.twitter.scalding.{Dsl, Mode, TypedPipe, Grouped, IterableSource, TupleSetter, TupleConverter}
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
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
    reducers: Int): PipeFactory[(K, V)]
}

object ScaldingStore extends java.io.Serializable {
  def empty[K,V]: FlowProducer[TypedPipe[(K, V)]] = {
    import Dsl._
    Reader({implicit fdm: (FlowDef, Mode) => TypedPipe.from(IterableSource(Iterable.empty[(K,V)])) })
  }
}

trait BatchedScaldingStore[K, V] extends ScaldingStore[K, V] { self =>
  /** The batcher for this store */
  def batcher: Batcher

  implicit def ordering: Ordering[K]

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

  /** we are guaranteed to have sufficient input and deltas to cover these batches
   * and that the batches are given in order
   */
  private def mergeBatched(input: FlowProducer[TypedPipe[(K,V)]], batches: List[BatchID],
    deltas: FlowToPipe[(K,V)], sg: Semigroup[V],
    commutativity: Commutativity, reducers: Int): FlowToPipe[(K,V)] = {

    //Convert to a Grouped by "swapping" Time and K
    def toGrouped(items: KeyValuePipe[K, V]): Grouped[K, (Long, V)] =
      items.groupBy { case (_, (k, _)) => k }
        .mapValues { case (t, (_, v)) => (t, v) }
        .withReducers(reducers)

    //Unswap the Time and K
    def toKVPipe(tp: TypedPipe[(K, (Long, V))]): KeyValuePipe[K, V] =
      tp.map { case (k, (t, v)) => (t, (k, v)) }

    // Return the items in this batch in ._1 and not in a future batch in ._2
    def split(b: BatchID, items: KeyValuePipe[K, V]): (KeyValuePipe[K, V], KeyValuePipe[K, V]) = {
      // we need cascading to see that we are forking this pipe:
      val forked = Scalding.forcePipe(items)
      (forked.filter { tkv => batcher.batchOf(new java.util.Date(tkv._1)) == b },
      forked.filter { tkv => batcher.batchOf(new java.util.Date(tkv._1)) > b })
    }

    // put the smallest time on these to ensure they come first in a sort:
    def withMinTime(p: TypedPipe[(K,V)]): KeyValuePipe[K, V] =
      p.map { t => (Long.MinValue, t) }

    Reader[FlowInput, KeyValuePipe[K, V]] { (flowMode: (FlowDef, Mode)) =>
      implicit val flowDef = flowMode._1
      implicit val mode = flowMode._2

      @annotation.tailrec
      def sumThem(head: BatchID,
        rest: List[BatchID],
        lastSummed: TypedPipe[(K, V)],
        restDeltas: KeyValuePipe[K, V],
        acc: List[KeyValuePipe[K, V]]): KeyValuePipe[K, V] = {
        val (theseDeltas, nextDeltas) = split(head, restDeltas)

        val grouped: Grouped[K, (Long, V)] = toGrouped(withMinTime(lastSummed) ++ theseDeltas)

        val sorted = grouped.sortBy { _._1 } // sort by time
        val maybeSorted = commutativity match {
          case Commutative => grouped // order does not matter
          case NonCommutative => sorted
        }

        val redFn: (((Long, V), (Long, V)) => (Long, V)) = { (left, right) =>
          val (tl, vl) = left
          val (tr, vr) = right
          (tl max tr, sg.plus(vl, vr))
        }
        // We strip the times
        val nextSummed = toKVPipe(maybeSorted.reduce(redFn)).values
        // could be an empty method, in which case scalding will do nothing here
        writeLast(head, nextSummed)

        // Make the incremental stream
        val stream = toKVPipe(sorted.scanLeft(None: Option[(Long, V)]) { (old, item) =>
            old match {
              case None => Some(item)
              case Some(prev) => Some(redFn(prev, item))
            }
          }
          .mapValueStream { _.flatten /* unbox the option */ }
          .toTypedPipe
        )
        rest match {
          // When there is no more, merge them all:
          case Nil => (stream :: acc).reduce { _ ++ _ }
          case nextHead :: nextTail =>
            sumThem(nextHead, nextTail, nextSummed, nextDeltas, stream :: acc)
        }
      }
      val latestSummed = input(flowMode)
      val incoming = deltas(flowMode)
      // This will throw if there is not one batch, but that should never happen and is a failure
      sumThem(batches.head, batches.tail, latestSummed, incoming, Nil)
    }
  }

  /** instances of this trait MAY NOT change the logic here. This always follows the rule
   * that we look for existing data (avoiding reading deltas in that case), then we fall
   * back to the last checkpointed output by calling readLast. In that case, we compute the
   * results by rolling forward
   */
  final override def merge(delta: PipeFactory[(K, V)],
    sg: Semigroup[V],
    commutativity: Commutativity,
    reducers: Int): PipeFactory[(K, V)] = StateWithError({ in: FactoryInput =>
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
                  val blist = BatchID.toIterable(batchesWeCanBuild).toList
                  val merged = mergeBatched(input, blist, deltaFlow2Pipe, sg, commutativity, reducers)
                  // it is a static (i.e. independent from input) bug if this get ever throws
                  val available = batchOps.intersect(blist, timeSpan).get
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
    else if (exclusiveUB == firstNonZero) Right((firstNonZero.prev, ScaldingStore.empty))
    else Left(List("Earliest batch set at :" + firstNonZero + " but tried to read: " + exclusiveUB))
  }
}
