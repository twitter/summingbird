/*
Copyright 2014 Twitter, Inc.

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

import java.util.concurrent.{ BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue }

import com.twitter.algebird.Monoid
import com.twitter.summingbird._
import com.twitter.summingbird.option.JobId
import org.scalacheck.Arbitrary
import org.scalatest.WordSpec

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Tests for Summingbird's in-memory planner.
 */

class ConcurrentMemoryLaws extends WordSpec {
  // This is dangerous, obviously. The Memory platform tested here
  // doesn't perform any batching, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)

  import scala.concurrent.ExecutionContext.Implicits.global

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def empty[T](b: BlockingQueue[T]): List[T] = {
    def go(items: List[T]): List[T] = b.poll() match {
      case null => items.reverse
      case x => go(x :: items)
    }
    go(Nil)
  }

  def unorderedEq[T](left: List[T], right: List[T]): Boolean = {
    val leftMap = left.groupBy(identity).mapValues(_.size)
    val rightMap = right.groupBy(identity).mapValues(_.size)
    val eqv = leftMap == rightMap
    if (!eqv) { println(s"from Queue: $leftMap\nfrom scala: $rightMap") }
    eqv
  }

  def testGraph[T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv] =
    new TestGraphs[ConcurrentMemory, T, K, V](new ConcurrentMemory)(
      () => new ConcurrentHashMap[K, V]())(() => new LinkedBlockingQueue[T]())(
      Producer.source[ConcurrentMemory, T](_))(s => { k => Option(s.get(k)) })({ (f, items) =>
      unorderedEq(empty(f), items)
    })({ (p: ConcurrentMemory, plan: ConcurrentMemoryPlan) => Await.result(plan.run, Duration.Inf) })

  /**
   * Tests the in-memory planner against a job with a single flatMap
   * operation.
   */
  def singleStepLaw[T: Arbitrary: Manifest, K: Arbitrary, V: Monoid: Arbitrary: Equiv] =
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

  def mapKeysChecker[T: Manifest: Arbitrary, K1: Arbitrary, K2: Arbitrary, V: Monoid: Arbitrary: Equiv](): Boolean = {
    val platform = new ConcurrentMemory
    val currentStore = new ConcurrentHashMap[K2, V]()
    val original = sample[List[T]]
    val fnA = sample[T => List[(K1, V)]]
    val fnB = sample[K1 => List[K2]]

    // Use the supplied platform to execute the source into the
    // supplied store.
    val plan = platform.plan {
      TestGraphs.singleStepMapKeysJob[ConcurrentMemory, T, K1, K2, V](original, currentStore)(fnA, fnB)
    }
    Await.result(plan.run, Duration.Inf)
    val lookupFn = { k: K2 => Option(currentStore.get(k)) };
    TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB).forall {
      case (k, v) =>
        val lv = lookupFn(k).getOrElse(Monoid.zero)
        Equiv[V].equiv(v, lv)
    }
  }

  def lookupCollectChecker[T: Arbitrary: Equiv: Manifest, U: Arbitrary: Equiv]: Boolean = {
    val mem = new ConcurrentMemory
    val input = sample[List[T]]
    val srv = sample[T => Option[U]]
    val buffer = new LinkedBlockingQueue[(T, U)]()
    val prod = TestGraphs.lookupJob[ConcurrentMemory, T, U](input, srv, buffer)
    Await.result(mem.plan(prod).run, Duration.Inf)
    // check it out:
    val buffData = empty(buffer)
    val correctData = TestGraphs.lookupJobInScala(input, srv)
    unorderedEq(buffData, correctData)
  }

  /**
   * Tests the in-memory planner against a job with a single flatMap
   * operation and some test counters
   */
  def counterChecker[T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv]: Boolean = {
    implicit val jobID: JobId = new JobId("concurrent.memory.job.testJobId")
    val mem = new ConcurrentMemory
    val input = sample[List[T]]

    val fn = sample[(T) => List[(K, V)]]
    val sourceMaker = ConcurrentMemory.toSource[T](_)
    val original = sample[List[T]]
    val source = sourceMaker(original)
    val store: ConcurrentMemory#Store[K, V] = new ConcurrentHashMap[K, V]()

    val prod = TestGraphs.jobWithStats[ConcurrentMemory, T, K, V](jobID, source, store)(t => fn(t))
    Await.result(mem.plan(prod).run, Duration.Inf)
    //mem.run(mem.plan(prod))

    val origCounter = mem.counter(Group("counter.test"), Name("orig_counter")).get
    val fmCounter = mem.counter(Group("counter.test"), Name("fm_counter")).get
    val fltrCounter = mem.counter(Group("counter.test"), Name("fltr_counter")).get

    (origCounter == original.size) &&
      (fmCounter == (original.flatMap(fn).size * 2)) &&
      (fltrCounter == (original.flatMap(fn).size))
  }

  "The ConcurrentMemory Platform" should {
    //Set up the job:
    "singleStep w/ Int, Int, Set[Int]" in { assert(singleStepLaw[Int, Int, Set[Int]] == true) }
    "singleStep w/ Int, String, List[Int]" in { assert(singleStepLaw[Int, String, List[Int]] == true) }
    "singleStep w/ String, Short, Map[Set[Int], Long]" in { assert(singleStepLaw[String, Short, Map[Set[Int], Long]] == true) }

    // Note the stored values only make sense if you have a commutative monoid
    // since, due to concurrency, we might put things in a different order with this platform
    "diamond w/ Int, Int, Set[Int]" in { assert(diamondLaw[Int, Int, Set[Int]] == true) }
    "diamond w/ String, Short, Map[Set[Int], Long]" in {
      /*
       * It is important to use an Equiv on Maps that treats empty like 0s, since our scala
       * implementation uses MapAlgebra.sumByKey, which for better or worse, removes zeros of
       * monoids, since that is what the MapMonoid does. :/
       */
      import com.twitter.algebird.MapAlgebra.sparseEquiv
      assert(diamondLaw[String, Short, Map[Set[Int], Long]] == true)
    }

    "leftJoin w/ Int, Int, String, Long, Set[Int]" in { assert(leftJoinLaw[Int, Int, String, Long, Set[Int]] == true) }

    "flatMapKeys w/ Int, Int, Int, Set[Int]" in { assert(mapKeysChecker[Int, Int, Int, Set[Int]] == true) }

    "lookupCollect w/ Int, Int" in { assert(lookupCollectChecker[Int, Int] == true) }

    "counters w/ Int, Int, Int" in { assert(counterChecker[Int, Int, Int] == true) }
  }

}
