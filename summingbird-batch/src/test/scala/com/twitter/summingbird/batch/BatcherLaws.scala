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

object BatcherLaws extends Properties("Batcher") {
  import Generators._

  def batchIdIdentity(batcher : Batcher) = { (b : BatchID) =>
    batcher.batchOf(batcher.earliestTimeOf(b))
  }

  property("UnitBatcher should always return the same batch") = {
    val batcher = Batcher.unit
    val ident = batchIdIdentity(batcher)
    forAll { batchID: BatchID => ident(batchID) == BatchID(0) }
  }

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
      hourlyBatcher.enclosedBy(BatchID(i), hourlyBatcher) == List(BatchID(i))
    }
}
