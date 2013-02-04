/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.batch

import com.twitter.scalding.{ Hours, Minutes, Seconds, Millisecs }

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._

object BatcherProperties extends Properties("Batcher") {

  def batchIdIdentity[Time](batcher : Batcher[Time]) =
    { (b : BatchID) => batcher.batchOf(batcher.earliestTimeOf(b)) }

  property("UnitBatcher should always return the same batch") = {
    val batcher = UnitBatcher[Int](Int.MinValue)
    val ident = batchIdIdentity(batcher)
    forAll { batchID: BatchID => ident(batchID) == BatchID(0) }
  }

  val secondsBatcher = SecondsDurationBatcher(Hours(24))

  property("SecondsDurationBatcher should know the ordering on Ints") =
    forAll { (a: Int, b: Int) =>
      secondsBatcher.timeComparator.compare(a, b) == a.compare(b)
    }

  property("SecondsDurationBatcher should batch correctly") =
    forAll { seconds: Int =>
      (seconds > 0) ==> {
        val dayIndex = seconds / 60 / 60 / 24
        secondsBatcher.batchOf(seconds) == BatchID(dayIndex)
      }
    }

  property("SecondsDurationBatcher should convert ints to longs") =
    forAll { i: Int =>
      (i >= 0) ==> (secondsBatcher.timeToMillis(i) == (i.toLong * 1000L))
    }

  val milliBatcher = MillisecondsDurationBatcher(Hours(1))

  property("MilliSecondsDurationBatcher should know the ordering on Longs") =
    forAll { (a: Int, b: Int) =>
      milliBatcher.timeComparator.compare(a, b) == a.compare(b)
    }

  property("MilliSecondsDurationBatcher should batch correctly") =
    check { millis: Long =>
      (millis > 0) ==> {
        val hourIndex: Long = millis / 1000 / 60 / 60
        milliBatcher.batchOf(millis) == BatchID(hourIndex)
      }
    }

  property("MilliSecondsDurationBatcher correctly roundtrip Longs") =
    check { millis: Long =>
      milliBatcher.timeToMillis(millis) == millis
    }
}
