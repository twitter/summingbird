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
import com.twitter.summingbird.option.JobId
import org.scalacheck.{ Arbitrary, _ }
import org.scalatest.WordSpec

import scala.collection.mutable.{ HashMap => MutableHashMap, ListBuffer, Map => MutableMap }
/**
 * Tests for Summingbird's in-memory planner.
 */

class MemoryLaws extends WordSpec {
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
    import MemoryArbitraries._
    val serviceFn: MemoryService[K, JoinedU] = Arbitrary.arbitrary[MemoryService[K, JoinedU]].sample.get
    testGraph[T, K, V].leftJoinChecker[U, JoinedU](serviceFn,
      { svc => { (k: K) => svc.get(k) } },
      sample[List[T]],
      sample[T => List[(K, U)]],
      sample[((K, (U, Option[JoinedU]))) => List[(K, V)]])
  }

  /**
   * Tests the in-memory planner by generating arbitrary flatMap and
   * service functions and joining against a store (independent of the join).
   */
  def leftJoinAgainstStoreChecker[T: Manifest: Arbitrary, K: Arbitrary, U: Arbitrary, JoinedU: Monoid: Arbitrary, V: Monoid: Arbitrary: Equiv] = {
    val platform = new Memory
    val finalStore: Memory#Store[K, V] = MutableMap.empty[K, V]
    val storeAndService: Memory#Store[K, JoinedU] with Memory#Service[K, JoinedU] = new MutableHashMap[K, JoinedU]() with MemoryService[K, JoinedU]
    val sourceMaker = Memory.toSource[T](_)
    val items1 = sample[List[T]]
    val items2 = sample[List[T]]

    val fnA = sample[(T) => List[(K, JoinedU)]]
    val fnB = sample[(T) => List[(K, U)]]
    val postJoinFn = sample[((K, (U, Option[JoinedU]))) => List[(K, V)]]

    val plan = platform.plan {
      TestGraphs.leftJoinWithStoreJob[Memory, T, T, U, K, JoinedU, V](sourceMaker(items1),
        sourceMaker(items2),
        storeAndService,
        finalStore)(fnA)(fnB)(postJoinFn)
    }
    platform.run(plan)
    val serviceFn = storeAndService.get(_)
    val lookupFn = finalStore.get(_)

    val storeAndServiceMatches = MapAlgebra.sumByKey(
      items1
        .flatMap(fnA)
    ).forall {
        case (k, v) =>
          val lv: JoinedU = serviceFn(k).getOrElse(Monoid.zero[JoinedU])
          Equiv[JoinedU].equiv(v, lv)
      }

    val finalStoreMatches = MapAlgebra.sumByKey(
      items2.
        flatMap(fnB)
        .map { case (k, u) => (k, (u, serviceFn(k))) }
        .flatMap(postJoinFn)
    ).forall {
        case (k, v) =>
          val lv = lookupFn(k).getOrElse(Monoid.zero[V])
          Equiv[V].equiv(v, lv)
      }

    storeAndServiceMatches && finalStoreMatches
  }

  def mapKeysChecker[T: Manifest: Arbitrary, K1: Arbitrary, K2: Arbitrary, V: Monoid: Arbitrary: Equiv](): Boolean = {
    val platform = new Memory
    val currentStore: Memory#Store[K2, V] = MutableMap.empty[K2, V]
    val sourceMaker = Memory.toSource[T](_)
    val original = sample[List[T]]
    val fnA = sample[T => List[(K1, V)]]
    val fnB = sample[K1 => List[K2]]

    // Use the supplied platform to execute the source into the
    // supplied store.
    val plan = platform.plan {
      TestGraphs.singleStepMapKeysJob[Memory, T, K1, K2, V](sourceMaker(original), currentStore)(fnA, fnB)
    }
    platform.run(plan)
    val lookupFn = currentStore.get(_)
    TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB).forall {
      case (k, v) =>
        val lv = lookupFn(k).getOrElse(Monoid.zero)
        Equiv[V].equiv(v, lv)
    }
  }

  def lookupCollectChecker[T: Arbitrary: Equiv: Manifest, U: Arbitrary: Equiv]: Boolean = {
    import MemoryArbitraries._
    val mem = new Memory
    val input = sample[List[T]]
    val srv = sample[MemoryService[T, U]]
    var buffer = Vector[(T, U)]() // closure to mutate this
    val prod = TestGraphs.lookupJob[Memory, T, U](Memory.toSource(input), srv, { tu: (T, U) => buffer = buffer :+ tu })
    mem.run(mem.plan(prod))
    // check it out:
    Equiv[List[(T, U)]].equiv((buffer.toList),
      TestGraphs.lookupJobInScala(input, { (t: T) => srv.get(t) }))
  }

  /**
   * Tests the in-memory planner against a job with a single flatMap
   * operation and some test counters
   */
  def counterChecker[T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv]: Boolean = {
    implicit val jobID: JobId = new JobId("memory.job.testJobId")
    val mem = new Memory
    val fn = sample[(T) => List[(K, V)]]
    val sourceMaker = Memory.toSource[T](_)
    val original = sample[List[T]]
    val source = sourceMaker(original)
    val store: Memory#Store[K, V] = MutableMap.empty[K, V]

    val prod = TestGraphs.jobWithStats[Memory, T, K, V](jobID, source, store)(t => fn(t))
    mem.run(mem.plan(prod))

    val origCounter = mem.counter(Group("counter.test"), Name("orig_counter")).get
    val fmCounter = mem.counter(Group("counter.test"), Name("fm_counter")).get
    val fltrCounter = mem.counter(Group("counter.test"), Name("fltr_counter")).get

    (origCounter == original.size) &&
      (fmCounter == (original.flatMap(fn).size * 2)) &&
      (fltrCounter == (original.flatMap(fn).size))
  }

  "The Memory Platform" should {
    //Set up the job:
    "singleStep w/ Int, Int, Set[Int]" in { assert(singleStepLaw[Int, Int, Set[Int]] == true) }
    "singleStep w/ Int, String, List[Int]" in { assert(singleStepLaw[Int, String, List[Int]] == true) }
    "singleStep w/ String, Short, Map[Set[Int], Long]" in { assert(singleStepLaw[String, Short, Map[Set[Int], Long]] == true) }

    "diamond w/ Int, Int, Set[Int]" in { assert(diamondLaw[Int, Int, Set[Int]] == true) }
    "diamond w/ Int, String, List[Int]" in { assert(diamondLaw[Int, String, List[Int]] == true) }
    "diamond w/ String, Short, Map[Set[Int], Long]" in { assert(diamondLaw[String, Short, Map[Set[Int], Long]] == true) }

    "leftJoin w/ Int, Int, String, Long, Set[Int]" in { assert(leftJoinLaw[Int, Int, String, Long, Set[Int]] == true) }
    "leftJoinAgainstStore w/ Int, Int, String, Long, Int" in { assert(leftJoinAgainstStoreChecker[Int, Int, String, Long, Int] == true) }

    "flatMapKeys w/ Int, Int, Int, Set[Int]" in { assert(mapKeysChecker[Int, Int, Int, Set[Int]] == true) }

    "lookupCollect w/ Int, Int" in { assert(lookupCollectChecker[Int, Int] == true) }

    "counters w/ Int, Int, Int" in { assert(counterChecker[Int, Int, Int] == true) }

    "needless also shouldn't cause a problem" in {
      val source = Memory.toSource(0 to 100)
      val store1 = MutableMap.empty[Int, Int]
      val store2 = MutableMap.empty[Int, Int]
      val comp = source.map(v => (v % 3, v)).sumByKey(store1)
      val prod = comp.also(comp.mapValues(_._2).write(new BufferFunc).sumByKey(store2))
      val mem = new Memory
      mem.run(mem.plan(prod))
      assert(store1.toMap == ((0 to 100).groupBy(_ % 3).mapValues(_.sum)))
      assert(store2.toMap == ((0 to 100).groupBy(_ % 3).mapValues(_.sum)))
    }
  }

}
