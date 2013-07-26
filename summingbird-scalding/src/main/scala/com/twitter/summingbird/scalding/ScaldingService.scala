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

import com.twitter.algebird.monad.{StateWithError, Reader}
import com.twitter.scalding.{Mode, TypedPipe}
import com.twitter.summingbird.batch.{ BatchID, Batcher, Interval }
import cascading.flow.FlowDef

trait ScaldingService[K, V] extends java.io.Serializable {
  // A static, or write-once service can  potentially optimize this without writing the (K, V) stream out
  def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))]
}

class EmptyService[K, V] extends ScaldingService[K, V] {
  def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))] =
    getKeys.map { _.map { _.map { case (t, (k, v)) => (t, (k, (v, None: Option[V]))) } } }
}

trait BatchedService[K, V] extends ScaldingService[K, V] {
  // The batcher that describes this service
  def batcher: Batcher
  def ordering: Ordering[K]

  /** Get the most recent last batch and the ID (strictly less than the input ID)
   * The "Last" is the stream with only the newest value for each key.
   */
  def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowToPipe[(K, V)])]

  /** Reads the key log for this batch. By key log we mean a log of time, key and value when the key was written
   * to. This is an associative operation and sufficient to scedule the service.
   * This only has the keys that changed value during this batch.
   */
  def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, Option[V])]]

  def reducers: Option[Int]

  protected def batchedLookup[W](covers: Interval[Time],
    getKeys: FlowToPipe[(K, W)],
    last: (BatchID, FlowToPipe[(K, V)]),
    streams: Iterable[(BatchID, FlowToPipe[(K, Option[V])])]): FlowToPipe[(K, (W, Option[V]))] =
      Reader[FlowInput, KeyValuePipe[K, (W, Option[V])]] { (flowMode: (FlowDef, Mode)) =>
        val left = getKeys(flowMode)
        val liftedLast: KeyValuePipe[K, Option[V]] = last._2(flowMode).map { case (t, (k, w)) => (t, (k, Some(w))) }
        // TODO (https://github.com/twitter/summingbird/issues/91): we
        // could not bother to load streams outside the covers, but
        // probably we aren't anyway assuming the time spans are not
        // wildly mismatched
        val right = streams.foldLeft(liftedLast) { (merged, item) =>
          merged ++ (item._2(flowMode))
        }
        implicit val ord = ordering
        def flatOpt[T](o: Option[Option[T]]): Option[T] = o.flatMap(identity)

        LookupJoin(left, right).map { case (t, (k, (w, optoptv))) => (t, (k, (w, flatOpt(optoptv)))) }
      }

  final def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))] =
    StateWithError({ (in: FactoryInput) =>
      val (timeSpan, mode) = in

      // This object combines some common scalding batching operations:
      val batchOps = new BatchedOperations(batcher)

      val coveringBatches = batchOps.coverIt(timeSpan)
      readLast(coveringBatches.min, mode).right.flatMap { batchLastFlow =>
        val (startingBatch, init) = batchLastFlow
        val streamBatches = BatchID.range(startingBatch.next, coveringBatches.max)
        val batchStreams = streamBatches.map { b => (b, readStream(b, mode)) }
        // only produce continuous output, so stop at the first none:
        val existing = batchStreams
          .takeWhile { _._2.isDefined }
          .collect { case (batch, Some(flow)) => (batch, flow) }

        if(existing.isEmpty) {
          Left(List("[ERROR] Could not load any batches of the service stream in: " + toString + " for: " + timeSpan.toString))
        }
        else {
          val inBatches = List(startingBatch) ++ existing.map { _._1 }
          val bInt = BatchID.toInterval(inBatches).get // by construction this is an interval, so this can't throw
          val toRead = batchOps.intersect(bInt, timeSpan) // No need to read more than this
          getKeys((toRead, mode))
            .right
            .map { case ((available, outM), getFlow) =>
              /*
               * Note we can open more batches than we need to join, but
               * we will deal with that when we do the join (by filtering first,
               * then grouping on (key, batchID) to parallelize.
               */
              ((available, outM),
                Scalding.limitTimes(available, batchedLookup(available, getFlow, batchLastFlow, existing)))
            }
        }
      }
    })
}
