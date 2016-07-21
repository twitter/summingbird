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

import cascading.flow.{ Flow, FlowDef }

import com.twitter.algebird._
import com.twitter.algebird.monad._
import com.twitter.summingbird.batch._
import com.twitter.summingbird.option.{ Commutative, NonCommutative, Commutativity }
import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

/**
 * Tests for BatchedStore
 */
object BatchedStoreProperties extends Properties("BatchedStore's Properties") {

  implicit def intersectionArb[T: Arbitrary: Ordering]: Arbitrary[Intersection[InclusiveLower, ExclusiveUpper, T]] =
    Arbitrary {
      for {
        l <- Arbitrary.arbitrary[T]
        u <- Arbitrary.arbitrary[T]
        if implicitly[Ordering[T]].lt(l, u)
      } yield Intersection(InclusiveLower(l), ExclusiveUpper(u))
    }

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary {
    Gen.choose(1L, 100000L)
      .map { Timestamp(_) }
  }

  implicit val arbitraryPipeFactory: Arbitrary[PipeFactory[Nothing]] = {
    Arbitrary {
      Gen.const {
        StateWithError[(Interval[Timestamp], Mode), List[FailureReason], FlowToPipe[Nothing]] {
          (timeMode: (Interval[Timestamp], Mode)) =>
            {
              val (time: Interval[Timestamp], mode: Mode) = timeMode
              val a: FlowToPipe[Nothing] = Reader { (fdM: (FlowDef, Mode)) => TypedPipe.empty }
              Right((timeMode, a))
            }
        }
      }
    }
  }

  implicit def timeExtractor[T <: (Long, Any)] = TestUtil.simpleTimeExtractor[T]

  implicit val arbitraryInputWithTimeStampAndBatcher: Arbitrary[(List[(Long, Int)], Batcher, TestStore[Int, Int])] = Arbitrary {
    for {
      arbInt <- Arbitrary.arbitrary[List[Int]]
      in = arbInt.zipWithIndex.map { case (item: Int, time: Int) => (time.toLong, item) }
      arbMap <- Arbitrary.arbitrary[Map[Int, Int]]
      batcher = TestUtil.randomBatcher(in)
      lastTimeStamp = in.size
      testStore = TestStore[Int, Int]("test", batcher, arbMap, lastTimeStamp)
    } yield (in, batcher, testStore)
  }

  implicit def arbitraryLocalMode: Arbitrary[Mode] = Arbitrary { Gen.const(Local(true)) }
  implicit def arbitraryCommutativity: Arbitrary[Commutativity] = Arbitrary {
    Gen.oneOf(Seq(Commutative, NonCommutative))
  }

  property("readAfterLastBatch should return interval starting from the last batch written") = {
    forAll {
      (diskPipeFactory: PipeFactory[Nothing],
      interval: Intersection[InclusiveLower, ExclusiveUpper, Timestamp],
      inputWithTimeStampAndBatcherAndStore: (List[(Long, Int)], Batcher, TestStore[Int, Int]),
      mode: Mode) =>
        val (inputWithTimeStamp, batcher, testStore) = inputWithTimeStampAndBatcherAndStore
        val result = testStore.readAfterLastBatch(diskPipeFactory)((interval, mode))

        result match {
          case Right(((Intersection(InclusiveLower(readIntervalLower), ExclusiveUpper(_)), _), _)) => {
            //readInterval should start from the last written interval in the store
            val start: Timestamp = batcher.earliestTimeOf(testStore.initBatch.next)
            implicitly[Ordering[Timestamp]].equiv(readIntervalLower, start)
          }
          case Right(_) => false
          case Left(_) => interval == Empty()
        }
    }
  }

  property("readAfterLastBatch should not extend the end of interval requested") = {
    forAll {
      (diskPipeFactory: PipeFactory[Nothing],
      interval: Intersection[InclusiveLower, ExclusiveUpper, Timestamp],
      inputWithTimeStampAndBatcherAndStore: (List[(Long, Int)], Batcher, TestStore[Int, Int]),
      mode: Mode) =>
        val (inputWithTimeStamp, batcher, testStore) = inputWithTimeStampAndBatcherAndStore
        val result = testStore.readAfterLastBatch(diskPipeFactory)((interval, mode))

        result match {
          case Right(((Intersection(InclusiveLower(_), ExclusiveUpper(readIntervalUpper)), _), _)) => {
            //readInterval should start from the last written interval in the store
            implicitly[Ordering[Timestamp]].lteq(readIntervalUpper, interval.upper.upper)
          }
          case Right(_) => false
          case Left(_) => interval == Empty()
        }
    }
  }

  property("the end of merged interval is never extended") = {
    forAll {
      (diskPipeFactory: PipeFactory[Nothing],
      interval: Intersection[InclusiveLower, ExclusiveUpper, Timestamp],
      inputWithTimeStampAndBatcherAndStore: (List[(Long, Int)], Batcher, TestStore[Int, Int]),
      commutativity: Commutativity,
      mode: Mode) =>
        val (inputWithTimeStamp, batcher, testStore) = inputWithTimeStampAndBatcherAndStore
        val mergeResult = testStore.merge(diskPipeFactory, implicitly[Semigroup[Int]], commutativity, 10)((interval, mode))
        mergeResult.isRight ==> {
          val Right(((Intersection(InclusiveLower(_), ExclusiveUpper(readIntervalUpper)), _), _)) = mergeResult
          val requestedEndingTimestamp: Timestamp = interval.upper.upper
          val readIntervalEndingTimestamp: Timestamp = readIntervalUpper
          implicitly[Ordering[Timestamp]].lteq(readIntervalEndingTimestamp, requestedEndingTimestamp)
        }
    }
  }

  property("should not merge if the time interval on disk(from diskPipeFactory) is smaller than one batch") = {
    //To test this property, it requires the length of the batcher is at least 2 millis, since we want
    //to create data that fits a batch partially
    def atLeast2MsBatcher(batcher: Batcher): Boolean = {
      batcher match {
        case b: MillisecondBatcher => b.durationMillis >= 2
        case _ => true
      }
    }
    forAll {
      (interval: Intersection[InclusiveLower, ExclusiveUpper, Timestamp],
      inputWithTimeStampAndBatcherAndStore: (List[(Long, Int)], Batcher, TestStore[Int, Int]),
      commutativity: Commutativity,
      mode: Mode) =>
        val (inputWithTimeStamp, batcher, testStore) = inputWithTimeStampAndBatcherAndStore
        (atLeast2MsBatcher(batcher)) ==> {
          val nextBatchEnding = batcher.latestTimeOf(testStore.initBatch.next)

          //this diskPipeFactory returns a time interval that ends before the ending of next batch, meaning there is not enough data for a new batch
          val diskPipeFactory = StateWithError[(Interval[Timestamp], Mode), List[FailureReason], FlowToPipe[(Int, Int)]] {
            (timeMode: (Interval[Timestamp], Mode)) =>
              {
                val (time: Interval[Timestamp], mode: Mode) = timeMode
                val Intersection(InclusiveLower(startRequestedTime), ExclusiveUpper(_)) = time

                //shrink the endTime so it does not cover a whole batch
                val onDiskEndTime: Long = Gen.choose(startRequestedTime.milliSinceEpoch, nextBatchEnding.milliSinceEpoch).sample.get

                val readTime: Interval[Timestamp] = if (startRequestedTime == nextBatchEnding)
                  Empty()
                else
                  Intersection(InclusiveLower(startRequestedTime), ExclusiveUpper(nextBatchEnding))

                val flowToPipe: FlowToPipe[(Int, Int)] = Reader { (fdM: (FlowDef, Mode)) => TypedPipe.from[(Timestamp, (Int, Int))](Seq((Timestamp(10), (2, 3)))) }
                Right(((readTime, mode), flowToPipe))
              }
          }

          val mergeResult = testStore.merge(diskPipeFactory, implicitly[Semigroup[Int]], commutativity, 10)((interval, mode))

          mergeResult match {
            case Left(l) => {
              l.mkString.contains("readTimespan is not convering at least one batch").label("fail with right reason")
            }
            case Right(_) => false.label("should fail when readTimespan is not covering at least one batch")
          }
        }
    }
  }
}
