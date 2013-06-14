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

import com.twitter.summingbird.monad.StateWithError
import com.twitter.summingbird.batch._

object TimeSpanBatcher {
  /**
   * Return the Interval of BatchIDs such that all the input times are
   * covered by these batches. DOES NOT change the Interval[Time]
   * Empty input is considered a failure
   */
  def coveringSet(batcher: Batcher): PlannerOutput[Interval[BatchID]] =
    StateWithError[FactoryInput, List[FailureReason], Interval[BatchID]]({ in: FactoryInput =>
      def truncateDown(ts: Time): BatchID =
        batcher.batchOf(new java.util.Date(ts))

      def truncateUp(ts: Time): BatchID = {
        val batch = batcher.batchOf(new java.util.Date(ts))
        if(batcher.earliestTimeOf(batch).getTime != ts) batch.next else batch
      }

      val (timeSpan, mode) = in
      timeSpan match {
        case Empty() => Left(List("Empty timespan into Batcher:" + batcher.toString))
        case Universe() => Right((in, Universe()))
        case ExclusiveUpper(upper) =>
          Right((in, ExclusiveUpper(truncateUp(upper))))
        case InclusiveLower(lower) =>
          Right((in, InclusiveLower(truncateDown(lower))))
        case interval@Intersection(InclusiveLower(l), ExclusiveUpper(u)) =>
          val upperBatch = truncateUp(u)
          val lowerBatch = truncateDown(l)
          Right((in, Interval.leftClosedRightOpen(lowerBatch, upperBatch)))
      }
  })
}

