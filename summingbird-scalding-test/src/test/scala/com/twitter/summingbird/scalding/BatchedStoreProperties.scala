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
import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

/**
 * Tests for Summingbird's Scalding planner.
 */

object BatchedStoreProperties extends Properties("BatchedStore's Properties") {

  implicit def intervalArb[T: Arbitrary: Ordering]: Arbitrary[Interval[T]] =
    Arbitrary {
      for {
        l <- Arbitrary.arbitrary[T]
        u <- Arbitrary.arbitrary[T]
      } yield Interval.leftClosedRightOpen(l, u)
    }

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary {
    Gen.choose(1L, 100000L)
      .map { Timestamp(_) }
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  implicit def arbitraryPipeFactory: Arbitrary[PipeFactory[(Int, Int)]] = {
    Arbitrary {
      Gen.const {
        StateWithError[(Interval[Timestamp], Mode), List[FailureReason], FlowToPipe[(Int, Int)]] {
          (timeMode: (Interval[Timestamp], Mode)) =>
            {
              val (time: Interval[Timestamp], mode: Mode) = timeMode
              val a: FlowToPipe[(Int, Int)] = Reader { (fdM: (FlowDef, Mode)) => TypedPipe.from[(Timestamp, (Int, Int))](Seq((Timestamp(10), (2, 3)))) }
              Right((timeMode, a))
            }
        }
      }
    }
  }

  implicit def timeExtractor[T <: (Long, Any)] = TestUtil.simpleTimeExtractor[T]

  implicit def arbitraryInputWithTimeStampAndBatcher: Arbitrary[(List[(Long, Int)], Batcher, TestStore[Int, Int])] = Arbitrary {
    Arbitrary.arbitrary[List[Int]]
      .map { in => in.zipWithIndex.map { case (item: Int, time: Int) => (time.toLong, item) } }
      .map { in =>
        val batcher = TestUtil.randomBatcher(in)
        val lastTimeStamp = in.size
        val testStore = TestStore[Int, Int]("test", batcher, sample[Map[Int, Int]], lastTimeStamp)
        (in, batcher, testStore)
      }
  }

  implicit def arbitraryLocalMode: Arbitrary[Mode] = Arbitrary { Gen.const(Local(true)) }

  property("readAfterLastBatch should return interval starting from the last batch written") = {
    forAll {
      (
      a: PipeFactory[(Int, Int)],
      interval: Interval[Timestamp],
      inputWithTimeStampAndBatcherAndStore: (List[(Long, Int)], Batcher, TestStore[Int, Int]),
      mode: Mode
    ) =>
        //      println(s"requested interval is ${interval}")

        val (inputWithTimeStamp, batcher, testStore) = inputWithTimeStampAndBatcherAndStore

        val result: PlannerOutput[(BatchID, FlowProducer[TypedPipe[(Int, Int)]], FlowToPipe[(Int, Int)])] = testStore.readAfterLastBatch(a)

        result((interval, mode)) match {
          case Right(((readInterval: Intersection[InclusiveLower, ExclusiveUpper, Timestamp], _), _)) => {
            println(s"readInterval is ${readInterval}, intersection is ${interval.intersect(readInterval)}")
            //readInterval should start from the last written interval
            val start: InclusiveLower[Timestamp] = InclusiveLower(batcher.earliestTimeOf(testStore.initBatch.next))
            readInterval.lower == start
          }
          case Left(_) => interval == Empty()
        }
    }
  }

  property("should merge Properly") = {
    forAll {
      (
      a: PipeFactory[(Int, Int)],
      interval: Interval[Timestamp],
      inputWithTimeStampAndBatcherAndStore: (List[(Long, Int)], Batcher, TestStore[Int, Int]),
      mode: Mode
    ) =>
        //println(s"requested interval is ${interval}")

        val (inputWithTimeStamp, batcher, testStore) = inputWithTimeStampAndBatcherAndStore
        val s: Semigroup[Int] = implicitly[Semigroup[Int]]
        println(testStore.merge(a, s, com.twitter.summingbird.option.Commutative, 10))
        1 == 1
    }
  }
}
