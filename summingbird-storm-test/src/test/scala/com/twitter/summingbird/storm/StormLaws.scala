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

package com.twitter.summingbird.storm

import com.twitter.summingbird._
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.option.LeftJoinGrouping
import org.scalatest.WordSpec
import org.scalacheck.Arbitrary
import com.twitter.summingbird.ArbitraryWorkaround._

/**
 * Tests for Summingbird's Storm planner.
 */
object StormLaws {
  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  implicit val storm = Storm.local(Map())

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  val nextFn = { pair: ((Int, (Int, Option[Int]))) =>
    val (k, (v, joinedV)) = pair
    List((k -> joinedV.getOrElse(10)))
  }

  val nextFn1 = { pair: ((Int, Option[Int])) =>
    val (v, joinedV) = pair
    List((joinedV.getOrElse(10)))
  }

  val serviceFn = sample[Int => Option[Int]]
}

// ALL TESTS START GO IN THE CLASS NOT OBJECT

class StormLaws extends WordSpec {
  import StormLaws._

  "StormPlatform matches Scala for single step jobs" in {
    val original = sample[List[Int]]
    val fn = sample[Int => List[(Int, Int)]]

    testProducer(TestGraphs.singleStepJob[TestPlatform, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.store[Int, Int]("store"))(fn))
  }

  "FlatMap to nothing" in {
    val original = sample[List[Int]]
    val fn = { (x: Int) => List[(Int, Int)]() }

    testProducer(TestGraphs.singleStepJob[TestPlatform, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.store[Int, Int]("store"))(fn))
  }

  "OptionMap and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = sample[Int => Option[Int]]
    val fnB = sample[Int => List[(Int, Int)]]

    testProducer(TestGraphs.twinStepOptionMapFlatMapJob[TestPlatform, Int, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.store[Int, Int]("store"))(fnA, fnB))
  }

  "OptionMap to nothing and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = { (x: Int) => None }
    val fnB = sample[Int => List[(Int, Int)]]

    testProducer(TestGraphs.twinStepOptionMapFlatMapJob[TestPlatform, Int, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.store[Int, Int]("store"))(fnA, fnB))
  }

  "StormPlatform matches Scala for large expansion single step jobs" in {
    val original = sample[List[Int]]
    val expander = sample[Int => List[(Int, Int)]]
    val expansionFunc = { (x: Int) =>
      expander(x).flatMap { case (k, v) => List((k, v), (k, v), (k, v), (k, v), (k, v)) }
    }

    testProducer(TestGraphs.singleStepJob[TestPlatform, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.store[Int, Int]("store"))(expansionFunc))
  }

  "StormPlatform matches Scala for flatmap keys jobs" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41) // sample[List[Int]]
    val fnA = sample[Int => List[(Int, Int)]]
    val fnB = sample[Int => List[Int]]

    testProducer(TestGraphs.singleStepMapKeysJob[TestPlatform, Int, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.store[Int, Int]("store"))(fnA, fnB))
  }

  "StormPlatform matches Scala for left join jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i, i)) }

    testProducer(TestGraphs.leftJoinJob[TestPlatform, Int, Int, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.service(serviceFn), TestPlatform.store("store"))(staticFunc)(nextFn))
  }

  "StormPlatform matches Scala for left join with flatMapValues jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i, i)) }

    testProducer(TestGraphs.leftJoinJobWithFlatMapValues[TestPlatform, Int, Int, Int, Int, Int](
          TestPlatform.source(original), TestPlatform.service(serviceFn), TestPlatform.store("store"))(staticFunc)(nextFn1))
  }

  "StormPlatform matches Scala for repeated tuple leftJoin jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i, i)) }

    testProducer(TestGraphs.repeatedTupleLeftJoinJob[TestPlatform, Int, Int, Int, Int, Int](
      TestPlatform.source(original), TestPlatform.service(serviceFn), TestPlatform.store("store"))(staticFunc)(nextFn))
  }

  "StormPlatform matches Scala for optionMap only jobs" in {
    val original = sample[List[Int]]

    testProducer(
      TestPlatform.source(original)
        .filter(_ % 2 == 0)
        .map(_ -> 10)
        .sumByKey(TestPlatform.store("store")))
  }

  "StormPlatform matches Scala for MapOnly/NoSummer" in {
    val original = sample[List[Int]]

    testProducer(TestGraphs.mapOnlyJob[TestPlatform, Int, Int](
      TestPlatform.source(original), TestPlatform.sink[Int]("sink"))(x => List(x)))
  }

  "StormPlatform with multiple summers" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41) // sample[List[Int]]

    testProducer(TestGraphs.multipleSummerJob[TestPlatform, Int, Int, Int, Int, Int, Int](
      TestPlatform.source(original),
      TestPlatform.store("store1"),
      TestPlatform.store("store2")
    )(x => List(x * 10), x => List((x, x)), x => List((x, x))))
  }

  "StormPlatform should be efficent in real world job" in {
    val original1 = sample[List[Int]]
    val original2 = sample[List[Int]]
    val original3 = sample[List[Int]]
    val original4 = sample[List[Int]]

    val fn1 = sample[(Int) => List[(Int, Int)]]
    val fn2 = sample[(Int) => List[(Int, Int)]]
    val fn3 = sample[(Int) => List[(Int, Int)]]

    val preJoinFn = sample[(Int) => (Int, Int)]
    val postJoinFn = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]

    val serviceFn = sample[Int => Option[Int]]

    testProducer(TestGraphs.realJoinTestJob[TestPlatform, Int, Int, Int, Int, Int, Int, Int, Int, Int](
      TestPlatform.source(original1), TestPlatform.source(original2), TestPlatform.source(original3), TestPlatform.source(original4),
      TestPlatform.service(serviceFn), TestPlatform.store("store"), fn1, fn2, fn3, preJoinFn, postJoinFn))
  }

  "StormPlatform should be able to handle two sumByKey's" in {
    testProducer(TestPlatform
      .source[Int](sample[List[Int]])
      .map((_, 1))
      .sumByKey(TestPlatform.store[Int, Int]("store1"))
      .sumByKey(TestPlatform.store[Int, (Option[Int], Int)]("store2")))
  }

  "StormPlatform should be able to handle AlsoProducer with Summer and FlatMap in different branches" in {
    val original = sample[List[(Int, Int)]]
    val branchFlatMap = sample[((Int, Int)) => List[(Int, Int)]]

    {
      val source = TestPlatform.source(original)
      testProducer(source.sumByKey(TestPlatform.store("store1")).also(
        source.flatMap(branchFlatMap).sumByKey(TestPlatform.store("store2"))
      ))
    }

    {
      val source = TestPlatform.source(original).flatMap(e => List(e))
      testProducer(source.sumByKey(TestPlatform.store("store1")).also(
        source.flatMap(branchFlatMap).sumByKey(TestPlatform.store("store2"))
      ))
    }

    {
      val source = TestPlatform.source(original).flatMap(e => List(e))
      testProducer(source.map(identity).sumByKey(TestPlatform.store("store1")).also(
        source.flatMap(branchFlatMap).sumByKey(TestPlatform.store("store2"))
      ))
    }

    {
      val source = TestPlatform.source(original)
        .sumByKey(TestPlatform.store("tmpStore")).map({ case (key, (_, value)) => (key, value) })
      testProducer(source.sumByKey(TestPlatform.store("store1")).also(
        source.flatMap(branchFlatMap).sumByKey(TestPlatform.store("store2"))
      ))
    }
  }

  "StormPlatform should work with grouped leftJoin" in {
    val leftJoinName = "leftJoin"
    val producer = TestPlatform.source(sample[List[Int]])
      .map(sample[Int => (Int, Int)])
      .leftJoin(TestPlatform.service(v => Some(v))).name("leftJoin")
      .mapValues { case (v, _) => v }
      .sumByKey(TestPlatform.store("store"))

    // Test without grouped left join:
    testProducer(producer)

    // And with:
    testProducer(producer)(Storm.local(Map(
      leftJoinName -> Options().set(LeftJoinGrouping.Grouped)
    )))
  }

  "StormPlatform should work with grouped leftJoin and summer" in {
    val source = TestPlatform.source(sample[List[Int]])
      .map(sample[Int => (Int, Int)])

    val producer1 = source
      .leftJoin(TestPlatform.service(v => Some(v)))
      .mapValues { case (v, _) => v }
      .sumByKey(TestPlatform.store("store1"))
    val producer2 = source.sumByKey(TestPlatform.store("store2"))

    testProducer(producer1.also(producer2))(Storm.local(Map(
      "DEFAULT" -> Options().set(LeftJoinGrouping.Grouped)
    )))
  }

  "StormPlatform should work with mapValues" in {
    testProducer(TestPlatform.source(sample[List[(Int, Int)]])
      .mapValues(identity)
      .sumByKey(TestPlatform.store("store")))
  }

  def testProducer[T](producer: TailProducer[TestPlatform, T])(implicit storm: Storm): Unit = {
    val memoryResult = MemoryTestExecutor(producer)
    val stormResult = StormTestExecutor(producer, storm)
    assertEquiv(memoryResult.stores, stormResult.stores)
    assertEquiv(memoryResult.sinks.keys, stormResult.sinks.keys)
    memoryResult.sinks.keys.foreach { key =>
      assertSinksAreTheSame(memoryResult.sinks(key), stormResult.sinks(key))
    }
  }

  def assertSinksAreTheSame[T](expected: List[T], returned: List[T]): Unit = {
    // We ignore order of elements in `Sinks` because it can be different on concurrent platforms.
    assert(expected.diff(returned).isEmpty)
    assert(returned.diff(expected).isEmpty)
  }

  def assertEquiv[T](expected: T, returned: T)(implicit equiv: Equiv[T]): Unit = {
    assert(equiv.equiv(expected, returned), (expected.toString, returned.toString))
  }
}
