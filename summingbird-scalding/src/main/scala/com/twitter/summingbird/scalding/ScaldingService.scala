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

import com.twitter.scalding.{Mode, TypedPipe}
import com.twitter.summingbird.batch.{ BatchID, Batcher, Interval }
import com.twitter.summingbird.monad.{StateWithError, Reader}
import cascading.flow.FlowDef

trait ScaldingService[K, V] {
  // A static, or write-once service can  potentially optimize this without writing the (K, V) stream out
  def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))]
}

trait BatchedService[K, V] extends ScaldingService[K, V] {
  // The batcher that describes this service
  def batcher: Batcher

  /** Reads the key log for this batch. By key log we mean a log of time, key and value when the key was written
   * to. This is an associative operation and sufficient to scedule the service.
   *
   * May include keys from previous batches, since we need to be able to look back in time to the most recent
   * value for each key that exists.
   */
  def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, V)]]

  // Implement this as either mapside or grouping join
  protected def batchedLookup[W](covers: Interval[Time],
      getKeys: FlowToPipe[(K,W)],
      batches: Iterable[(BatchID, FlowToPipe[(K,V)])]): FlowToPipe[(K,(W,Option[V]))]

  final def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))] =
    StateWithError({ (in: FactoryInput) =>
      val (timeSpan, mode) = in

      // This object combines some common scalding batching operations:
      val batchOps = new BatchedOperations(batcher)

      val batchStreams = batchOps.coverIt(timeSpan).map { b => (b, readStream(b, mode)) }
      // only produce continuous output, so stop at the first none:
      val existing = batchStreams
        .takeWhile { _._2.isDefined }
        .collect { case (batch, Some(flow)) => (batch, flow) }

      if(existing.isEmpty) {
        Left(List("[ERROR] Could not load any batches of the service stream in: " + toString + " for: " + timeSpan.toString))
      }
      else {
        val inBatches = existing.map { _._1 }
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
            ((available, outM), batchedLookup(available, getFlow, existing))
          }
      }
    })
}
