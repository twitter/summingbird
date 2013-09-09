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
import org.specs._

import java.util.Date

import com.twitter.algebird.{Interval, Empty}

object BatcherLaws extends Properties("Batcher") {
  import Generators._

  def batchIdIdentity(batcher : Batcher) = { (b : BatchID) =>
    batcher.batchOf(batcher.earliestTimeOf(b))
  }

  def earliestIsLE(batcher: Batcher) = forAll { (d: Date) =>
    (batcher.earliestTimeOf(batcher.batchOf(d)).compareTo(d) <= 0)
  }

  def batchesAreWeakOrderings(batcher: Batcher) = forAll { (d1: Date, d2: Date) =>
    batcher.batchOf(d1).compare(batcher.batchOf(d2)) match {
      case 0 => true // can't say much
      case x => d1.compareTo(d2) == x
    }
  }

  def batchesIncreaseByAtMostOne(batcher: Batcher) = forAll { (d: Date) =>
    val nextTimeB = batcher.batchOf(new Date(d.getTime + 1L))
    batcher.batchOf(d) == nextTimeB ||
      batcher.batchOf(d) == nextTimeB.prev
  }

  def batchesCoveredByIdent(batcher: Batcher) =
    forAll { (d: Date) =>
      val b = batcher.batchOf(d)
      val list = BatchID.toIterable(
        batcher.batchesCoveredBy(batcher.toInterval(b))
      ).toList
      list == List(b)
    }

  def batcherLaws(batcher: Batcher) =
    earliestIsLE(batcher) &&
      batchesAreWeakOrderings(batcher) &&
      batchesIncreaseByAtMostOne(batcher) &&
      batchesCoveredByIdent(batcher)

  property("UnitBatcher should always return the same batch") = {
    val batcher = Batcher.unit
    val ident = batchIdIdentity(batcher)
    forAll { batchID: BatchID => ident(batchID) == BatchID(0) }
  }

  property("Unit obeys laws") = batcherLaws(Batcher.unit)

  property("1H obeys laws") = batcherLaws(Batcher.ofHours(1))

  property("Combined obeys laws") =
    batcherLaws(new CombinedBatcher(Batcher.ofHours(1), new Date(), Batcher.ofMinutes(10)))

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

      (hourlyBatcher.batchOf(new Date(millis)) == flooredBatch) &&
      (hourlyBatcher.earliestTimeOf(flooredBatch).getTime ==
        hourlyBatchFloor(flooredBatch.id))
    }

  property("DurationBatcher should fully enclose each batch with a single batch") =
    forAll { i: Int =>
      hourlyBatcher.enclosedBy(BatchID(i), hourlyBatcher).toList == List(BatchID(i))
    }

  property("batchesCoveredBy is a subset of covers") =
    forAll { (int: Interval[Date]) =>
      val coveredBy = hourlyBatcher.batchesCoveredBy(int)
      val covers = hourlyBatcher.cover(int)
      (covers && coveredBy) == coveredBy
    }

  property("batchesCoveredBy produces has times in the interval") =
    forAll { (d: Date, sl: SmallLong) =>
      // Make sure we cover at least an hour in the usual case by multiplying by ms per hour
      val int = Interval.leftClosedRightOpen(d, new Date(d.getTime + (60L * 60L * 1000L * sl.get)))
      val covered = hourlyBatcher.batchesCoveredBy(int)
      // If we have empty, we have to have <= 1 hour blocks, 2 or more must cover a 1 hour block
      ((covered == Empty[BatchID]()) && (sl.get <= 1)) || {
        val minBatch = BatchID.toIterable(covered).min
        val maxBatch = BatchID.toIterable(covered).max
        int.contains(hourlyBatcher.earliestTimeOf(minBatch)) &&
          int.contains(new Date(hourlyBatcher.earliestTimeOf(maxBatch.next).getTime - 1L))
      }
    }
}
