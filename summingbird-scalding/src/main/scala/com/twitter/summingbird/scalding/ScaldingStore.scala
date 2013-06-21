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

import com.twitter.summingbird.batch.{ BatchID, Batcher, Interval }
import com.twitter.summingbird.monad.{StateWithError, Reader}
import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.bijection.{Injection, Bijection, Conversion}
import com.twitter.scalding.{Mode, TypedPipe, Grouped}
import cascading.flow.FlowDef

import java.util.{Date => JDate}

import Conversion.asMethod

// TODO this functionality should be in algebird
sealed trait Commutativity extends java.io.Serializable
object NonCommutative extends Commutativity
object Commutative extends Commutativity

trait ScaldingStore[K, V] {
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

trait BatchedScaldingStore[K, V] extends ScaldingStore[K, V] {
  /** The batcher for this store */
  def batcher: Batcher

  implicit def ordering[K]: Ordering[K]

  /** If this full stream (result of a merge) for this batch is already materialized, return it
   */
  def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, V)]]

  /** Get the most recent last batch and the ID (strictly less than the input ID)
   * The "Last" is the stream with only the oldest value for each key, within the batch
   * combining the last from batchID and the deltas from batchID.next you get the stream
   * for batchID.next
   */
  def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowToPipe[(K, V)])]

  /** Instances may choose to write out the last or just compute it from the stream */
  def writeLast(batchID: BatchID, lastVals: KeyValuePipe[K, V])(implicit flowDef: FlowDef, mode: Mode): Unit = { }

  /** Instances may choose to write out materialized streams
   * by implementing this. This is what readStream returns.
   */
  def writeStream(batchID: BatchID, stream: KeyValuePipe[K, V])(implicit flowDef: FlowDef, mode: Mode): Unit = { }

  /** we are guaranteed to have sufficient input and deltas to cover these batches
   * and that the batches are given in order
   */
  private def mergeBatched(input: FlowToPipe[(K,V)], batches: List[BatchID],
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
    def split(b: BatchID, items: KeyValuePipe[K, V]): (KeyValuePipe[K, V], KeyValuePipe[K, V]) =
      (items.filter { tkv => batcher.batchOf(new java.util.Date(tkv._1)) == b },
      items.filter { tkv => batcher.batchOf(new java.util.Date(tkv._1)) > b })

    Reader[FlowInput, KeyValuePipe[K, V]] { (flowMode: (FlowDef, Mode)) =>
      implicit val flowDef = flowMode._1
      implicit val mode = flowMode._2

      @annotation.tailrec
      def sumThem(head: BatchID,
        rest: List[BatchID],
        lastSummed: KeyValuePipe[K, V],
        restDeltas: KeyValuePipe[K, V],
        acc: List[KeyValuePipe[K, V]]): KeyValuePipe[K, V] = {
        val (theseDeltas, nextDeltas) = split(head, restDeltas)

        val grouped: Grouped[K, (Long, V)] = toGrouped(lastSummed ++ theseDeltas)

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
        val nextSummed = toKVPipe(maybeSorted.reduce(redFn))
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
        // could be empty, in which case scalding will do nothing here
        writeStream(head, stream)
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

      implicit val t2dBij = Bijection.build { bint: Interval[Time] =>
        bint.mapNonDecreasing { new JDate(_) } } { bint: Interval[JDate] =>
        bint.mapNonDecreasing { _.getTime }
      }

      val (timeSpan, mode) = in
      val batchInterval = batcher.cover(timeSpan.as[Interval[JDate]])
      val coveringBatches = BatchID.toIterable(batchInterval)
      val batchStreams = coveringBatches.map { bid => (bid, readStream(bid, mode)) }

      // This data is already on disk and will not be recomputed
      val existing = batchStreams
        .takeWhile { _._2.isDefined }
        .collect { case (batch, Some(flow)) => (batch, flow) }

      // Maybe an inclusive interval of batches to build
      val batchesToBuild: Option[(BatchID, BatchID)] = batchStreams
          .dropWhile { _._2.isDefined }
          .map { _._1 }
          .toList match {
            case Nil => None
            case list => Some((list.min, list.max))
          }

      def batchToTime(bint: Interval[BatchID]): Interval[Time] =
        bint.mapNonDecreasing { batcher.earliestTimeOf(_).getTime }

      // If there are any new batches to build, try to build as many as possible
      val maybeBuilt: Option[Try[(Iterable[BatchID], FlowToPipe[(K,V)])]] = batchesToBuild
        .map { case (firstNewBatch, lastNewBatch) =>
          readLast(firstNewBatch, mode)
            .right
            .flatMap { case (actualLast, input) =>
              val firstDeltaBatch = actualLast.next
              // Compute the times we need to read of the deltas
              val deltaBatches = Interval.leftClosedRightOpen(firstDeltaBatch, lastNewBatch.next)
              val deltaTimes = batchToTime(deltaBatches)
              // Read the delta stream for the needed times
              delta((deltaTimes, mode))
                .right
                .flatMap { case ((availableInput, innerm), deltaFlow2Pipe) =>
                  val batchesWeCanBuild = batcher.batchesCoveredBy(availableInput)
                  val batchesWeCanBuildIt = BatchID.toIterable(batchesWeCanBuild)
                  // Check that deltas needed can actually be loaded going back to the first new batch
                  if (batchesWeCanBuildIt.headOption != Some(firstDeltaBatch)) {
                    Left(List("Cannot load an entire initial batch: " + firstDeltaBatch.toString
                      + " of deltas at: " + this.toString))
                  }
                  else
                    Right((batchesWeCanBuildIt,
                      mergeBatched(input, batchesWeCanBuildIt.toList, deltaFlow2Pipe, sg, commutativity, reducers)))
                }
            }
        }

      def mergeExistingAndBuilt(optBuilt: Option[(Iterable[BatchID], FlowToPipe[(K,V)])]): Try[((Interval[Time], Mode), FlowToPipe[(K,V)])] = {
        val (aBatches, aFlows) = existing.unzip
        val flows = aFlows ++ (optBuilt.map { _._2 })
        val batches = aBatches ++ (optBuilt.map { _._1 }.getOrElse(Iterable.empty))

        if (flows.isEmpty)
          Left(List("Zero batches requested, should never occur: " + timeSpan.toString))
        else {
          val available = batchToTime(BatchID.toInterval(batches).get) && timeSpan
          val merged = Scalding.limitTimes(available, flows.reduce(Scalding.merge(_, _)))
          Right(((available, mode), merged))
        }
      }

      maybeBuilt match {
        case None => mergeExistingAndBuilt(None)
        case Some(Left(err)) => if (existing.isEmpty) Left(err) else mergeExistingAndBuilt(None)
        case Some(Right(built)) => mergeExistingAndBuilt(Some(built))
      }
    })
}
