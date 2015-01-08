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
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._
import collection.mutable.{ Map => MutableMap, ListBuffer, HashMap => MutableHashMap }

import org.specs2.mutable._
import org.scalacheck._
import Gen._
/**
 * Tests for Summingbird's in-memory planner.
 */

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
        val m = new MutableHashMap[K, V]() with MemoryService[K, V]
        k.zip(v).foreach(p => m.put(p._1, p._2))
        m
      }
    )
}

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
    "singleStep w/ Int, Int, Set[Int]" in { singleStepLaw[Int, Int, Set[Int]] must beTrue }
    "singleStep w/ Int, String, List[Int]" in { singleStepLaw[Int, String, List[Int]] must beTrue }
    "singleStep w/ String, Short, Map[Set[Int], Long]" in { singleStepLaw[String, Short, Map[Set[Int], Long]] must beTrue }

    "diamond w/ Int, Int, Set[Int]" in { diamondLaw[Int, Int, Set[Int]] must beTrue }
    "diamond w/ Int, String, List[Int]" in { diamondLaw[Int, String, List[Int]] must beTrue }
    "diamond w/ String, Short, Map[Set[Int], Long]" in { diamondLaw[String, Short, Map[Set[Int], Long]] must beTrue }

    "leftJoin w/ Int, Int, String, Long, Set[Int]" in { leftJoinLaw[Int, Int, String, Long, Set[Int]] must beTrue }
    "leftJoinAgainstStore w/ Int, Int, String, Long, Int" in { leftJoinAgainstStoreChecker[Int, Int, String, Long, Int] must beTrue }

    "flatMapKeys w/ Int, Int, Int, Set[Int]" in { mapKeysChecker[Int, Int, Int, Set[Int]] must beTrue }

    "lookupCollect w/ Int, Int" in { lookupCollectChecker[Int, Int] must beTrue }

    "counters w/ Int, Int, Int" in { counterChecker[Int, Int, Int] must beTrue }
  }

}
