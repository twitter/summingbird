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

package com.twitter.summingbird.batch

import scala.collection.immutable.SortedSet
import java.util.{ Comparator, Date }
import java.util.concurrent.TimeUnit
import java.io.Serializable

/**
 * For the purposes of batching, each Event object has exactly one
 * Time (in millis). The Batcher uses this time to assign each Event
 * to a specific BatchID. A Batcher can return the minimum time for
 * each BatchID.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object Batcher {
  /**
    * Returns a batcher that assigns batches based on multiples of the
    * supplied TimeUnit from the epoch.
    */
  def apply(value: Long, unit: TimeUnit) =
    new MillisecondBatcher(unit.toMillis(value))

  /**
    * Returns a batcher that generates batches of the supplied number
    * of minutes.
    */
  def ofMinutes(count: Long) = Batcher(count, TimeUnit.MINUTES)

  /**
    * Returns a batcher that generates batches of the supplied number
    * of hours.
    */
  def ofHours(count: Long) = Batcher(count, TimeUnit.HOURS)

  /**
    * Returns a batcher that generates batches of the supplied number
    * of days.
    */
  def ofDays(count: Long) = Batcher(count, TimeUnit.DAYS)

    /**
    * Returns a batcher that assigns every input tuple to the same
    * batch.
    */
  val unit = new AbstractBatcher  {
    def batchOf(t: Date) = BatchID(0)
    def earliestTimeOf(batch: BatchID) = new Date(0)
  }
}

trait Batcher extends Serializable {
  /** Returns the batch into which the supplied Date is bucketed. */
  def batchOf(t: Date): BatchID

  private def truncateDown(ts: Date): BatchID = batchOf(ts)

  private def truncateUp(ts: Date): BatchID = {
    val batch = batchOf(ts)
    if(earliestTimeOf(batch) != ts) batch.next else batch
  }

  /** Return the largest interval of batches completely covered by
   * the interval of time.
   */
  def batchesCoveredBy(interval: Interval[Date]): Interval[BatchID] =
    interval match {
      case Empty() => Empty()
      case Universe() => Universe()
      case ExclusiveUpper(upper) => ExclusiveUpper(truncateDown(upper))
      case InclusiveLower(lower) => InclusiveLower(truncateUp(lower))
      case Intersection(InclusiveLower(l), ExclusiveUpper(u)) =>
        val upperBatch = truncateDown(u)
        val lowerBatch = truncateUp(l)
        Interval.leftClosedRightOpen(lowerBatch, upperBatch)
    }

  /** Returns the (inclusive) earliest time of the supplied batch. */
  def earliestTimeOf(batch: BatchID): Date

  /** Returns the current BatchID. */
  def currentBatch: BatchID = batchOf(new Date())

  /** What batches are needed to cover the given interval
   * or: for all t in interval, batchOf(t) is in the result
   */
  def cover(interval: Interval[Date]): Interval[BatchID] =
    interval match {
      case Empty() => Empty()
      case Universe() => Universe()
      case ExclusiveUpper(upper) => ExclusiveUpper(truncateUp(upper))
      case InclusiveLower(lower) => InclusiveLower(truncateDown(lower))
      case Intersection(InclusiveLower(l), ExclusiveUpper(u)) =>
        val upperBatch = truncateUp(u)
        val lowerBatch = truncateDown(l)
        Interval.leftClosedRightOpen(lowerBatch, upperBatch)
    }

  /**
    * Returns the sequence of BatchIDs that the supplied `other`
    * batcher would need to fetch to fully enclose the supplied
    * `batchID`.
    */
  def enclosedBy(batchID: BatchID, other: Batcher): Iterable[BatchID] = {
    val earliestInclusive = earliestTimeOf(batchID)
    val latestInclusive = new Date(earliestTimeOf(batchID.next).getTime - 1L)
    BatchID.range(
      other.batchOf(earliestInclusive),
      other.batchOf(latestInclusive)
    ).toList
  }

  def enclosedBy(extremities: (BatchID, BatchID), other: Batcher): Iterable[BatchID] = {
    val (bottom, top) = extremities
    SortedSet(
      BatchID.range(bottom, top).toSeq
        .flatMap(enclosedBy(_, other)):_*
    )
  }
}

/**
  * Abstract class to extend for easier java interop.
  */
abstract class AbstractBatcher extends Batcher
