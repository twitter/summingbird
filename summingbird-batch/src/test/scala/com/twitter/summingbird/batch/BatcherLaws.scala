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

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._
import org.scalatest.WordSpec

import com.twitter.summingbird.batch._
import com.twitter.algebird.{ Interval, ExclusiveUpper, Empty }
import java.util.concurrent.TimeUnit

object BatcherLaws extends Properties("Batcher") {
  import Generators._
  import OrderedFromOrderingExt._

  def batchIdIdentity(batcher: Batcher) = { (b: BatchID) =>
    batcher.batchOf(batcher.earliestTimeOf(b))
  }

  def earliestIs_<=(batcher: Batcher) = forAll { (d: Timestamp) =>
    val ord = implicitly[Ordering[Timestamp]]
    ord.compare(batcher.earliestTimeOf(batcher.batchOf(d)), d) <= 0
  }

  def batchesAreWeakOrderings(batcher: Batcher) = forAll { (d1: Timestamp, d2: Timestamp) =>
    val ord = implicitly[Ordering[BatchID]]
    val ordT = implicitly[Ordering[Timestamp]]
    ord.compare(batcher.batchOf(d1), batcher.batchOf(d2)) match {
      case 0 => true // can't say much
      case x => ordT.compare(d1, d2) == x
    }
  }

  def batchesIncreaseByAtMostOne(batcher: Batcher) = forAll { (d: Timestamp) =>
    val nextTimeB = batcher.batchOf(d.next)
    batcher.batchOf(d) == nextTimeB ||
      batcher.batchOf(d) == nextTimeB.prev
  }

  def batchesCoveredByIdent(batcher: Batcher) =
    forAll { (d: Timestamp) =>
      val b = batcher.batchOf(d)
      val list = BatchID.toIterable(
        batcher.batchesCoveredBy(batcher.toInterval(b))
      ).toList
      list == List(b)
    }

  def batchIntervalTransformToTs(batcher: Batcher, intervalGenerator: (BatchID, BatchID) => Interval[BatchID]) =
    forAll { (tsA: Timestamp, tsB: Timestamp, deltaMs: Long) =>
      val (tsLower, tsUpper) = if (tsA < tsB) (tsA, tsB) else (tsB, tsA)

      val deltaBounded = Milliseconds(deltaMs % 1000 * 86000 * 365L)
      val int = intervalGenerator(batcher.batchOf(tsLower), batcher.batchOf(tsUpper))

      val tsInterval = batcher.toTimestamp(int)

      val generatedTS = tsLower + deltaBounded
      val generatedBatch = batcher.batchOf(generatedTS)

      // Granularity of the batcher interval is bigger
      // So we can't correctly do both intersections together
      int.contains(generatedBatch) == tsInterval.contains(generatedTS)
    }

  def batcherLaws(batcher: Batcher) =
    earliestIs_<=(batcher) &&
      batchesAreWeakOrderings(batcher) &&
      batchesIncreaseByAtMostOne(batcher) &&
      batchesCoveredByIdent(batcher) &&
      batchIntervalTransformToTs(batcher, Interval.leftOpenRightClosed(_, _)) &&
      batchIntervalTransformToTs(batcher, Interval.leftClosedRightOpen(_, _))

  property("UnitBatcher should always return the same batch") = {
    val batcher = Batcher.unit
    val ident = batchIdIdentity(batcher)
    forAll { batchID: BatchID => ident(batchID) == BatchID(0) }
  }

  property("Unit obeys laws") = batcherLaws(Batcher.unit)

  property("1H obeys laws") = batcherLaws(Batcher.ofHours(1))
  property("UTC 1H obeys laws") = batcherLaws(CalendarBatcher.ofHoursUtc(1))
  property("UTC 1D obeys laws") = batcherLaws(CalendarBatcher.ofDaysUtc(1))

  property("Combined obeys laws") =
    batcherLaws(new CombinedBatcher(Batcher.ofHours(1), ExclusiveUpper(Timestamp.now), Batcher.ofMinutes(10)))

  val millisPerHour = 1000 * 60 * 60

  def hourlyBatchFloor(batchIdx: Long): Long =
    if (batchIdx >= 0)
      batchIdx * millisPerHour
    else
      batchIdx * millisPerHour + 1

  val hourlyBatcher = Batcher.ofHours(1)

  property("DurationBatcher should batch correctly") =
    forAll { millis: Long =>
      val hourIndex: Long = millis / millisPerHour

      // Long division rounds toward zero. Add a correction to make
      // sure that our index is floored toward negative inf.
      val flooredBatch = BatchID(if (millis < 0) (hourIndex - 1) else hourIndex)

      (hourlyBatcher.batchOf(Timestamp(millis)) == flooredBatch) &&
        (hourlyBatcher.earliestTimeOf(flooredBatch).milliSinceEpoch ==
          hourlyBatchFloor(flooredBatch.id))
    }

  property("DurationBatcher should fully enclose each batch with a single batch") =
    forAll { i: Int =>
      hourlyBatcher.enclosedBy(BatchID(i), hourlyBatcher).toList == List(BatchID(i))
    }

  property("batchesCoveredBy is a subset of covers") =
    forAll { (int: Interval[Timestamp]) =>
      val coveredBy = hourlyBatcher.batchesCoveredBy(int)
      val covers = hourlyBatcher.cover(int)
      (covers && coveredBy) == coveredBy
    }

  property("Lower batch edge should align") = {
    implicit val tenSecondBatcher = Batcher(10, TimeUnit.SECONDS)
    forAll { initialTime: Int =>
      initialTime > 0 ==> {
        Stream.iterate(Timestamp(initialTime * 1000L))(_.incrementSeconds(1))
          .take(100).forall { t =>
            if (t.milliSinceEpoch % (1000 * 10) == 0)
              tenSecondBatcher.isLowerBatchEdge(t)
            else !tenSecondBatcher.isLowerBatchEdge(t)
          }
      }
    }
  }

  property("batchesCoveredBy produces has times in the interval") =
    forAll { (d: Timestamp, sl: SmallLong) =>
      // Make sure we cover at least an hour in the usual case by multiplying by ms per hour
      val int = Interval.leftClosedRightOpen(d, d.incrementHours(sl.get))
      val covered = hourlyBatcher.batchesCoveredBy(int)
      // If we have empty, we have to have <= 1 hour blocks, 2 or more must cover a 1 hour block
      ((covered == Empty[BatchID]()) && (sl.get <= 1)) || {
        val minBatch = BatchID.toIterable(covered).min
        val maxBatch = BatchID.toIterable(covered).max
        int.contains(hourlyBatcher.earliestTimeOf(minBatch)) &&
          int.contains(hourlyBatcher.earliestTimeOf(maxBatch.next).prev)
      }
    }

}
