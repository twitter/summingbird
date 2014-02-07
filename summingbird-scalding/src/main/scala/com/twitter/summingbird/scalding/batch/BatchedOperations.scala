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

import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.bijection.{Injection, Bijection, Conversion}
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.scalding._
import com.twitter.scalding.Mode

import Conversion.asMethod

/** Services and Stores are very similar, but not exact.
 * This shares the logic for them.
 */
private class BatchedOperations(batcher: Batcher) {

  def coverIt[T](timeSpan: Interval[Timestamp]): Iterable[BatchID] = {
    val batchInterval = batcher.cover(timeSpan.as[Interval[Timestamp]])
    BatchID.toIterable(batchInterval)
  }

  def batchToTimestamp(bint: Interval[BatchID]): Interval[Timestamp] =
     bint.mapNonDecreasing { batcher.earliestTimeOf(_) }

  def intersect(batches: Interval[BatchID], ts: Interval[Timestamp]): Interval[Timestamp] =
    batchToTimestamp(batches) && ts

  def intersect(batches: Iterable[BatchID], ts: Interval[Timestamp]): Option[Interval[Timestamp]] =
    BatchID.toInterval(batches).map { intersect(_, ts) }

  def readBatched[T](inBatches: Interval[BatchID], mode: Mode, in: PipeFactory[T]): Try[(Interval[BatchID], FlowToPipe[T])] = {
    val inTimes = batchToTimestamp(inBatches)
    // Read the delta stream for the needed times
    in((inTimes, mode))
      .right
      .map { case ((availableInput, innerm), f2p) =>
        val batchesWeCanBuild = batcher.batchesCoveredBy(availableInput.as[Interval[Timestamp]])
        (batchesWeCanBuild, f2p)
      }
  }
}
