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

package com.twitter.summingbird.scalding.batch

import cascading.flow.{ Flow, FlowDef }

import com.twitter.algebird._
import com.twitter.algebird.monad._
import com.twitter.summingbird.batch._
import com.twitter.summingbird.option.{ Commutative, NonCommutative, Commutativity }
import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

object LTuple2Properties extends Properties("LTuple2 Properties") {

  property("When the contents equal then the LTuple2 equals") = {
    forAll { (a1: Int, b1: Int, a2: Int, b2: Int) =>

      val ltup1 = LTuple2(a1, b1)
      val ltup2 = LTuple2(a2, b2)

      if (a1 == a2 && b1 == b2)
        ltup1 == ltup2
      else
        ltup1 != ltup2
    }
  }

  property("Case of always equal values, should equal") = {
    forAll { (a1: Int, b1: Int) =>

      val ltup1 = LTuple2(a1, b1)
      val ltup2 = LTuple2(a1, b1)

      ltup1 == ltup2
    }
  }

  property("Case of always equal values, should equal. Different types.") = {
    forAll { (a1: Int, b1: String) =>

      val ltup1 = LTuple2(a1, b1)
      val ltup2 = LTuple2(a1, b1)

      ltup1 == ltup2
    }
  }

  property("things when tuple2 of different types equals then LTuple2 equals too") = {
    forAll { (a1: Int, b1: String, a2: Int, b2: String) =>

      val ltup1 = LTuple2(a1, b1)
      val ltup2 = LTuple2(a2, b2)

      if (a1 == a2 && b1 == b2)
        ltup1 == ltup2
      else
        ltup1 != ltup2
    }
  }

  property("Hash code of the LTuple2 is the same as a tuple2 would have been") = {
    forAll { (a1: Int, b1: String) =>

      val ltup1 = LTuple2(a1, b1)

      val tup1 = (a1, b1)
      tup1.hashCode == ltup1.hashCode
    }
  }

}
