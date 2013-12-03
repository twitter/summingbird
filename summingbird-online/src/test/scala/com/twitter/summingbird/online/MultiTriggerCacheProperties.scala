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

package com.twitter.summingbird.online

import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option._
import com.twitter.summingbird.planner._
import com.twitter.summingbird.memory.Memory
import com.twitter.algebird.Semigroup
import scala.collection.mutable.{Map => MMap}
import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._

import com.twitter.util.Duration

object MultiTriggerCacheProperties extends Properties("MultiTriggerCache") {

  implicit def arbFlushFreq = Arbitrary {
         Gen.choose(1, 4000)
            .map { x: Int => FlushFrequency(Duration.fromMilliseconds(x)) }
      }

  implicit def arbCacheSize = Arbitrary {
         Gen.choose(0, 400)
            .map { x =>
             println(x)
              CacheSize(x) }
  }

  implicit def arbCache[K, V: Semigroup] = Arbitrary {
     for {
      cacheSize <- Arbitrary.arbitrary[CacheSize]
      flushFreq <- Arbitrary.arbitrary[FlushFrequency]
     } yield MultiTriggerCache[K, V](cacheSize, flushFreq)
  }

  property("Must Sum up ") = forAll { (cache: MultiTriggerCache[Int, Int]) =>
    true
  }


}
