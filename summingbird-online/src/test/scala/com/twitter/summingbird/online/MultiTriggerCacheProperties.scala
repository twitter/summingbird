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
import com.twitter.algebird.{MapAlgebra, Semigroup}
import com.twitter.util.{Future, Await}
import scala.collection.mutable.{Map => MMap}
import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._
import scala.util.Random
import com.twitter.util.Duration

object MultiTriggerCacheProperties extends Properties("MultiTriggerCache") {

  implicit def arbFlushFreq = Arbitrary {
         Gen.choose(1, 4000)
            .map { x: Int => FlushFrequency(Duration.fromMilliseconds(x)) }
      }

  implicit def arbCacheSize = Arbitrary {
         Gen.choose(0, 10)
            .map { x =>
              CacheSize(x) }
  }

   implicit def arbValueCombinerCacheSize = Arbitrary {
         Gen.choose(1, 10)
            .map { x =>
              ValueCombinerCacheSize(x) }
  }

  implicit def arbAsyncPoolSize = Arbitrary {
         Gen.choose(0, 5)
            .map { x =>
              AsyncPoolSize(x) }
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  property("Summing with and without the cache should match") = forAll { inputs: List[List[(Int, Int)]] =>
    val cache = new MultiTriggerCache[Int, Int](sample[CacheSize], sample[ValueCombinerCacheSize], sample[FlushFrequency], SoftMemoryFlushPercent(80.0F), sample[AsyncPoolSize])
    val reference = MapAlgebra.sumByKey(inputs.flatten)
    val resA = Await.result(Future.collect(inputs.map(cache.insert(_)))).map(_.toList).flatten
    val resB = Await.result(cache.forceTick)
    val other = MapAlgebra.sumByKey(resA.toList ++ resB.toList)
    val res = Equiv[Map[Int, Int]].equiv(
      reference,
      other
    )
    Await.ready(cache.cleanup)
    res
  }

  property("Input Set must not get duplicates") = forAll { (ids: Set[Int], inputs: List[List[(Int, Int)]]) =>
    val cache = new MultiTriggerCache[Int, (List[Int], Int)](sample[CacheSize], sample[ValueCombinerCacheSize], sample[FlushFrequency], SoftMemoryFlushPercent(80.0F), sample[AsyncPoolSize])
    val idList = (ids ++ Set(1)).toList
    var refCount = MMap[Int, Int]()
    val realInputs = inputs.map{ iList =>
        iList.map{ case (k, v) =>
          val id = idList(Random.nextInt(idList.size))
          refCount += (id -> (refCount.getOrElse(id, 0) + 1))
          (k, (List(id), v))
        }
    }.toList

    val reference = MapAlgebra.sumByKey(realInputs.flatten).mapValues(tupV => (tupV._1.sorted, tupV._2))
    val resA = realInputs.map(cache.insert(_)).map(Await.result(_)).map(_.toList).flatten
    val resB = Await.result(cache.forceTick)
    val other = MapAlgebra.sumByKey(resA.toList ++ resB.toList).mapValues(tupV => (tupV._1.sorted, tupV._2))
    Await.ready(cache.cleanup)

    val equiv = Equiv[Map[Int, (List[Int], Int)]].equiv(
      reference,
      other
    )
    if(equiv) {
      val postFreq = MapAlgebra.sumByKey(other.map(_._2._1).flatten.map((_, 1)))
      Equiv[Map[Int, Int]].equiv(
        refCount.toMap,
        postFreq
        )
    } else {
      equiv
    }
  }
}
