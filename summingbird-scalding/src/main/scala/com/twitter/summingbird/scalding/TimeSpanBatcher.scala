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
  // Return the batches completely covered by this pipe
  // as a subset of the given time interval
  // or fail if we cannot return at least one
  def asBatchRange(batcher: Batcher): PlannerOutput[(BatchID, BatchID)] =
    StateWithError[FactoryInput, List[FailureReason], (BatchID, BatchID)]({ in: FactoryInput =>
      def truncateDown(ts: Time): (Time, BatchID) = {
        val batch = batcher.batchOf(new java.util.Date(ts))
        (batcher.earliestTimeOf(batch).getTime, batch)
      }
      def truncateUp(ts: Time): (Time, BatchID) = {
        val batch = batcher.batchOf(new java.util.Date(ts))
        val up = if(batcher.earliestTimeOf(batch).getTime != ts) batch.next else batch
        (batcher.earliestTimeOf(up).getTime, up)
      }

      val (timeSpan, mode) = in
      timeSpan match {
        case Empty() => Left(List("Empty timespan into Batcher:" + batcher.toString))
        case Universe() => Right(((Universe(), mode), (BatchID.Min, BatchID.Max)))
        case ExclusiveUpper(upper) =>
          val (upperTime, upperBatch) = truncateDown(upper)
          Right(((ExclusiveUpper(upperTime), mode), (BatchID.Min, upperBatch)))
        case InclusiveLower(lower) =>
          val (lowerTime, lowerBatch) = truncateUp(lower)
          Right(((InclusiveLower(lowerTime), mode), (lowerBatch, BatchID.Max)))
        case interval@Intersection(InclusiveLower(l), ExclusiveUpper(u)) =>
          val (upperTime, upperBatch) = truncateDown(u)
          val (lowerTime, lowerBatch) = truncateUp(l)
          Interval.leftClosedRightOpen(lowerTime, upperTime) match {
            case Empty() => Left(List("Timespan is not a full batch: " + interval.toString + " batcher:"  + batcher.toString))
            case result => Right(((result, mode), (lowerBatch, upperBatch)))
          }
      }
  })
}

