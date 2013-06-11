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

package com.twitter.summingbird.memory

import com.twitter.algebird.MapAlgebra
import com.twitter.algebird.Monoid
import com.twitter.summingbird._
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._
import collection.mutable.{ Map => MutableMap }

/**
  * Tests for Summingbird's in-memory planner.
  */

object MemoryLaws extends Properties("Memory") {
  import TestGraphs._

  // This is dangerous, obviously. The Memory platform tested here
  // doesn't perform any batching, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)

  /**
    * Tests the in-memory planner against a job with a single flatMap
    * operation.
    */
  def singleStepLaw[T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv] =
    singleStepChecker[Memory, T, K, V](new Memory, MutableMap.empty[K, V])(Memory.toSource(_))(_.toMap)

  property("MemoryPlanner singleStep w/ Int, Int, Set[Int]") = singleStepLaw[Int, Int, Set[Int]]
  property("MemoryPlanner singleStep w/ Int, String, List[Int]") = singleStepLaw[Int, String, List[Int]]
  property("MemoryPlanner singleStep w/ String, Short, Map[Set[Int], Long]") = singleStepLaw[String, Short, Map[Set[Int], Long]]

  /**
    * Tests the in-memory planner by generating arbitrary flatMap and
    * service functions.
    */
  def leftJoinLaw[T: Manifest: Arbitrary, K: Arbitrary, U: Arbitrary, JoinedU: Arbitrary, V: Monoid: Arbitrary: Equiv] = {
    val serviceFn = Arbitrary.arbitrary[K => Option[JoinedU]].sample.get
    leftJoinChecker[Memory, T, K, U, JoinedU, V](new Memory, serviceFn, MutableMap.empty[K, V])(
      Memory.toSource(_))(identity)(_.toMap)
  }

  property("MemoryPlanner leftJoin w/ Int, Int, String, Long, Set[Int]") = leftJoinLaw[Int, Int, String, Long, Set[Int]]
}
