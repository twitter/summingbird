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

import com.twitter.algebird.{ MapAlgebra, Monoid }
import com.twitter.summingbird._
import com.twitter.summingbird.option._
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._
import collection.mutable.{ Map => MutableMap, ListBuffer }

import org.specs2.mutable._

/**
  * Tests for Summingbird's in-memory planner.
  */

object MemoryLaws extends Specification {
  // This is dangerous, obviously. The Memory platform tested here
  // doesn't perform any batching, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)

  class BufferFunc[T] extends (T => Unit) {
    val buf = ListBuffer[T]()
    def apply(t: T) = buf += t
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def testGraph[T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv] =
    new TestGraphs[Memory, T, K, V](new Memory)(
      () => MutableMap.empty[K, V])(() => new BufferFunc[T])(
      Memory.toSource(_))(s => { s.get(_) })({ (f, items) =>
        f.asInstanceOf[BufferFunc[T]].buf.toList == items
      })({ (p: Memory, plan: Memory#Plan[_]) => p.run(plan) })

  /**
    * Tests the in-memory planner against a job with a single flatMap
    * operation.
    */
  def singleStepLaw[T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv] =
    testGraph[T, K, V].singleStepChecker(sample[List[T]], sample[T => List[(K, V)]])

  /**
    * Tests the in-memory planner against a job with a single flatMap
    * operation.
    */
  def diamondLaw[T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv] =
    testGraph[T, K, V].diamondChecker(sample[List[T]], sample[T => List[(K, V)]], sample[T => List[(K, V)]])


  /**
    * Tests the in-memory planner by generating arbitrary flatMap and
    * service functions.
    */
  def leftJoinLaw[T: Manifest: Arbitrary, K: Arbitrary, U: Arbitrary, JoinedU: Arbitrary, V: Monoid: Arbitrary: Equiv] = {
    val serviceFn = Arbitrary.arbitrary[K => Option[JoinedU]].sample.get
    testGraph[T, K, V].leftJoinChecker[U, JoinedU](serviceFn, identity, sample[List[T]], sample[T => List[(K, U)]], sample[((K, (U, Option[JoinedU]))) => List[(K, V)]])
  }


  def mapKeysChecker[T: Manifest: Arbitrary, K1: Arbitrary, K2: Arbitrary,
               V: Monoid: Arbitrary: Equiv](): Boolean = {
    val platform = new Memory
    val currentStore: Memory#Store[K2, V] = MutableMap.empty[K2, V]
    val sourceMaker = Memory.toSource[T](_)
    val original = sample[List[T]]
    val fnA =  sample[T => List[(K1, V)]]
    val fnB = sample[K1 => List[K2]]

    // Use the supplied platform to execute the source into the
    // supplied store.
     val plan = platform.plan {
       TestGraphs.singleStepMapKeysJob[Memory, T, K1, K2, V](sourceMaker(original), currentStore)(fnA, fnB)
     }
     platform.run(plan)
    val lookupFn = currentStore.get(_)
    TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB).forall { case (k, v) =>
      val lv = lookupFn(k).getOrElse(Monoid.zero)
      Equiv[V].equiv(v, lv)
    }
  }

  def mapKeysCheckStats[T: Manifest: Arbitrary, K: Arbitrary,
               V: Monoid: Arbitrary: Equiv](): Boolean = {
    val currentStore: Memory#Store[K, V] = MutableMap.empty[K, V]
    val sourceMaker = Memory.toSource[T](_)
    val original = sample[List[T]]
    val fnA = sample[T => List[(K, V)]]

    // Use the supplied platform to execute the source into the
    // supplied store.
    val (stats, tail) = TestGraphs.singleStepMapKeysStatJob[Memory, T, K, V](sourceMaker(original), currentStore)(fnA)
    val platform = new Memory(Map("DEFAULT" -> Options().set(StatList(stats))))
    platform.run(platform.plan(tail))
    MemoryStats.get(stats.head) == original.size
  }

  def lookupCollectChecker[T:Arbitrary:Equiv:Manifest, U:Arbitrary:Equiv]: Boolean = {
    val mem = new Memory
    val input = sample[List[T]]
    val srv = sample[T => Option[U]]
    var buffer = Vector[(T,U)]() // closure to mutate this
    val prod = TestGraphs.lookupJob[Memory,T,U](Memory.toSource(input), srv, { tu: (T,U) => buffer = buffer :+ tu })
    mem.run(mem.plan(prod))
    // check it out:
    Equiv[List[(T,U)]].equiv((buffer.toList),
      TestGraphs.lookupJobInScala(input, srv))
  }

  "The Memory Platform" should {
    //Set up the job:
    "singleStep w/ Int, Int, Set[Int]" in { singleStepLaw[Int, Int, Set[Int]] must beTrue }
    "singleStep w/ Int, String, List[Int]" in { singleStepLaw[Int, String, List[Int]] must beTrue }
    "singleStep w/ String, Short, Map[Set[Int], Long]" in {singleStepLaw[String, Short, Map[Set[Int], Long]] must beTrue }

    "diamond w/ Int, Int, Set[Int]" in { diamondLaw[Int, Int, Set[Int]] must beTrue }
    "diamond w/ Int, String, List[Int]" in { diamondLaw[Int, String, List[Int]] must beTrue }
    "diamond w/ String, Short, Map[Set[Int], Long]" in { diamondLaw[String, Short, Map[Set[Int], Long]] must beTrue }

    "leftJoin w/ Int, Int, String, Long, Set[Int]" in { leftJoinLaw[Int, Int, String, Long, Set[Int]] must beTrue }

    "flatMapKeys w/ Int, Int, Int, Set[Int]" in { mapKeysChecker[Int, Int, Int, Set[Int]] must beTrue }
    "Memory Stats w/ Int, Int, Set[Int]" in { mapKeysCheckStats[Int, Int, Set[Int]] must beTrue }

    "lookupCollect w/ Int, Int" in { lookupCollectChecker[Int, Int] must beTrue }
  }

}
