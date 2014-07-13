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

import com.twitter.summingbird.scalding._
import source.{ TimePathedSource => BTimePathedSource }
import com.twitter.summingbird._
import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._
import scala.math._
import com.twitter.scalding.{ DateRange, RichDate }

case class TestData(requestedRange: DateRange, availableRange: DateRange, embiggen: Long)

object TimePathSourceLaws extends Properties("Time path source") {

  implicit def arbRateRange: Arbitrary[DateRange] = Arbitrary(for {
    startTs <- Gen.choose(-137878042589500L, 137878042589500L)
    startDate <- RichDate(startTs)
    endTsDelta <- Gen.choose(10L, 137878042589500L)
    endDate <- RichDate(startTs + endTsDelta)
  } yield DateRange(startDate, endDate))

  implicit def arbData = Arbitrary(for {
    reqRange <- arbitrary[DateRange]
    availableRange <- arbitrary[DateRange]
    embiggenVal <- Gen.choose(1L, 100000L)
    embiggen <- Gen.oneOf(embiggenVal, 0L)
  } yield TestData(reqRange, availableRange, embiggen))

  def genEmbiggen(embiggen: Long): (DateRange => DateRange) = {
    ((dr: DateRange) => DateRange(RichDate(dr.start.timestamp - embiggen), RichDate(dr.end.timestamp + embiggen)))
  }

  def genVertractor(availableRange: DateRange): (DateRange => Option[DateRange]) = {
    { (dr: DateRange) =>
      val botTs = max(dr.start.timestamp, availableRange.start.timestamp)
      val topTs = min(dr.end.timestamp, availableRange.end.timestamp)
      if (botTs > topTs) None else Some(DateRange(RichDate(botTs), RichDate(topTs)))
    }
  }

  def rangeWithEmbgginContained(smaller: DateRange, embiggen: Long, bigger: DateRange) = {
    bigger.contains(genEmbiggen(embiggen)(smaller))
  }

  def rangeLength(dr: DateRange): Long = dr.end.timestamp - dr.start.timestamp + 1

  property("if the reqRange + embiggen is inside the avail range, return should == requested") = forAll { (data: TestData) =>
    val retData = BTimePathedSource.minify(genEmbiggen(data.embiggen), genVertractor(data.availableRange))(data.requestedRange)
    if (rangeWithEmbgginContained(data.requestedRange, data.embiggen, data.availableRange)) {
      retData == Some(data.requestedRange)
    } else true // not tested here
  }

  property("If not a complete subset, but overlapping we can imply a few prerequisites") = forAll { (data: TestData) =>
    val retData = BTimePathedSource.minify(genEmbiggen(data.embiggen), genVertractor(data.availableRange))(data.requestedRange)
    retData match {
      case None => true
      case Some(range) =>
        (data.availableRange.contains(range)
          && data.requestedRange.contains(range)
          && rangeLength(range) > 0
        )
    }
  }

  property("If the return is none, then the ranges should be disjoint or one is None") = forAll { (data: TestData) =>
    val retData = BTimePathedSource.minify(genEmbiggen(data.embiggen), genVertractor(data.availableRange))(data.requestedRange)
    retData match {
      case None =>
        (
          rangeLength(data.requestedRange) == 0
          || rangeLength(data.availableRange) == 0
          || data.requestedRange.isBefore(data.availableRange.start)
          || data.requestedRange.isAfter(data.availableRange.end) // Disjoint
        )
      case Some(_) => true // Not in this test
    }
  }

}

