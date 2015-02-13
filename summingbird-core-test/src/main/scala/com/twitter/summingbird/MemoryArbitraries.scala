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
package com.twitter.summingbird

import com.twitter.summingbird.{ KeyedProducer, Producer }
import com.twitter.summingbird.memory.{ MemoryService, Memory }
import org.scalacheck.{ Gen, Arbitrary }

import scala.collection.mutable.HashMap

object MemoryArbitraries {
  implicit def arbSource1[K: Arbitrary]: Arbitrary[Producer[Memory, K]] =
    Arbitrary(Gen.listOfN(100, Arbitrary.arbitrary[K]).map(Producer.source[Memory, K](_)))
  implicit def arbSource2[K: Arbitrary, V: Arbitrary]: Arbitrary[KeyedProducer[Memory, K, V]] =
    Arbitrary(Gen.listOfN(100, Arbitrary.arbitrary[(K, V)]).map(Producer.source[Memory, (K, V)](_)))
  implicit def arbService[K: Arbitrary, V: Arbitrary]: Arbitrary[MemoryService[K, V]] =
    Arbitrary(
      for {
        k <- Gen.listOfN(100, Arbitrary.arbitrary[K])
        v <- Gen.listOfN(100, Arbitrary.arbitrary[V])
      } yield {
        val m = new HashMap[K, V]() with MemoryService[K, V]
        k.zip(v).foreach(p => m.put(p._1, p._2))
        m
      }
    )
}
