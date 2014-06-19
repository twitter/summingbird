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

import com.twitter.algebird.{
  Universe,
  Empty,
  Interval,
  Intersection,
  InclusiveLower,
  ExclusiveUpper,
  InclusiveUpper,
  ExclusiveLower,
  Lower,
  Upper
}

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
  val unit: Batcher = new AbstractBatcher {
    override val currentBatch = BatchID(0L)
    def batchOf(t: Timestamp) = currentBatch
    def earliestTimeOf(batch: BatchID) = Timestamp.Min

    override def latestTimeOf(batch: BatchID) = Timestamp.Max

    override def toInterval(b: BatchID): Interval[Timestamp] =
      if (b == BatchID(0))
        Intersection(
          InclusiveLower(Timestamp.Min),
          InclusiveUpper(Timestamp.Max)
        )
      else
        Empty[Timestamp]()

    val totalBatchInterval = Intersection(
      InclusiveLower(currentBatch), ExclusiveUpper(currentBatch.next)
    )
    override def batchesCoveredBy(interval: Interval[Timestamp]): Interval[BatchID] =
      interval match {
        case Empty() => Empty()
        case Universe() => totalBatchInterval
        case ExclusiveUpper(upper) => Empty()
        case InclusiveLower(lower) =>
          if (lower == Timestamp.Min) totalBatchInterval
          else Empty()
        case InclusiveUpper(upper) =>
          if (upper == Timestamp.Max) totalBatchInterval
          else Empty()
        case ExclusiveLower(lower) => Empty()
        case Intersection(low, high) => batchesCoveredBy(low) && batchesCoveredBy(high)
      }

    override def cover(interval: Interval[Timestamp]): Interval[BatchID] =
      interval match {
        case Empty() => Empty()
        case _ => totalBatchInterval
      }
  }
}

trait Batcher extends Serializable {
  /** Returns the batch into which the supplied Date is bucketed. */
  def batchOf(t: Timestamp): BatchID

  private def truncateDown(ts: Timestamp): BatchID = batchOf(ts)

  private def truncateUp(ts: Timestamp): BatchID = {
    val batch = batchOf(ts)
    if (earliestTimeOf(batch) != ts) batch.next else batch
  }

  /**
   * Return the largest interval of batches completely covered by
   * the interval of time.
   */
  private def dateToBatch(interval: Interval[Timestamp])(onIncLow: (Timestamp) => BatchID)(onExcUp: (Timestamp) => BatchID): Interval[BatchID] = {

    interval match {
      case Empty() => Empty()
      case Universe() => Universe()
      case ExclusiveUpper(upper) => ExclusiveUpper(onExcUp(upper))
      case InclusiveLower(lower) => InclusiveLower(onIncLow(lower))
      case InclusiveUpper(upper) => ExclusiveUpper(onExcUp(upper.next))
      case ExclusiveLower(lower) => InclusiveLower(onIncLow(lower.next))
      case Intersection(low, high) =>
        // Convert to inclusive:
        val lowdate = low match {
          case InclusiveLower(lb) => lb
          case ExclusiveLower(lb) => lb.next
        }
        //convert it exclusive:
        val highdate = high match {
          case InclusiveUpper(hb) => hb.next
          case ExclusiveUpper(hb) => hb
        }
        val upperBatch = onExcUp(highdate)
        val lowerBatch = onIncLow(lowdate)
        Interval.leftClosedRightOpen(lowerBatch, upperBatch)
    }
  }

  /**
   * Returns true if the supplied timestamp sits at the floor of the
   * supplied batch.
   */
  def isLowerBatchEdge(ts: Timestamp): Boolean =
    !BatchID.equiv.equiv(batchOf(ts), batchOf(ts.prev))

  def batchesCoveredBy(interval: Interval[Timestamp]): Interval[BatchID] =
    dateToBatch(interval)(truncateUp)(truncateDown)

  def toInterval(b: BatchID): Interval[Timestamp] =
    Intersection(InclusiveLower(earliestTimeOf(b)), ExclusiveUpper(earliestTimeOf(b.next)))

  def toTimestamp(b: Interval[BatchID]): Interval[Timestamp] =
    b match {
      case Empty() => Empty[Timestamp]()
      case Universe() => Universe[Timestamp]()
      case ExclusiveUpper(upper) => ExclusiveUpper(earliestTimeOf(upper))
      case InclusiveUpper(upper) => InclusiveUpper(latestTimeOf(upper))
      case InclusiveLower(lower) => InclusiveLower(earliestTimeOf(lower))
      case ExclusiveLower(lower) => ExclusiveLower(latestTimeOf(lower))
      case Intersection(low, high) => toTimestamp(low) && toTimestamp(high)
    }

  /** Returns the (inclusive) earliest time of the supplied batch. */
  def earliestTimeOf(batch: BatchID): Timestamp

  /** Returns the latest time in the given batch */
  def latestTimeOf(batch: BatchID): Timestamp = earliestTimeOf(batch.next).prev

  /** Returns the current BatchID. */
  def currentBatch: BatchID = batchOf(Timestamp.now)

  /**
   * What batches are needed to cover the given interval
   * or: for all t in interval, batchOf(t) is in the result
   */
  def cover(interval: Interval[Timestamp]): Interval[BatchID] =
    dateToBatch(interval)(truncateDown)(truncateUp)

  /**
   * Returns the sequence of BatchIDs that the supplied `other`
   * batcher would need to fetch to fully enclose the supplied
   * `batchID`.
   */
  def enclosedBy(batchID: BatchID, other: Batcher): Iterable[BatchID] = {
    val earliestInclusive = earliestTimeOf(batchID)
    val latestInclusive = latestTimeOf(batchID)
    BatchID.range(
      other.batchOf(earliestInclusive),
      other.batchOf(latestInclusive)
    )
  }

  def enclosedBy(extremities: (BatchID, BatchID), other: Batcher): Iterable[BatchID] = {
    val (bottom, top) = extremities
    SortedSet(
      BatchID.range(bottom, top).toSeq
        .flatMap(enclosedBy(_, other)): _*
    )
  }
}

/**
 * Abstract class to extend for easier java interop.
 */
abstract class AbstractBatcher extends Batcher
