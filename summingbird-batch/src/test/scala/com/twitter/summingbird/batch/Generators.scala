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

import org.scalacheck.Arbitrary
import org.scalacheck.Gen

import com.twitter.algebird.Interval

/**
  * Generators useful in testing Summingbird's batch module.
  */
object Generators {
  implicit val batchIdArb: Arbitrary[BatchID] =
    Arbitrary { Arbitrary.arbitrary[Long].map { BatchID(_) } }


  implicit val arbTimestamp : Arbitrary[Timestamp] = Arbitrary {
      // a relevant 200 or so year range
      Gen.choose(-137878042589500L, 137878042589500L)
        .map { Timestamp(_) }
    }


  implicit val dateArb: Arbitrary[java.util.Date] =
    Arbitrary {
      // a relevant 200 or so year range
      Gen.choose(-137878042589500L, 137878042589500L)
        .map { new java.util.Date(_) }
    }

  implicit def intervalArb[T:Arbitrary:Ordering]: Arbitrary[Interval[T]] =
    Arbitrary {
      for {
        l <- Arbitrary.arbitrary[T]
        u <- Arbitrary.arbitrary[T]
      } yield Interval.leftClosedRightOpen(l, u)
    }

  case class SmallLong(get: Long)
  implicit val smallLong: Arbitrary[SmallLong] =
    Arbitrary {
      for {
        v <- Gen.choose(-100L, 100L)
      } yield SmallLong(v)
    }
}
