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

import org.apache.storm.LocalCluster
import com.twitter.algebird.{MapAlgebra, Semigroup}
import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.summingbird.online._
import com.twitter.summingbird.memory._
import com.twitter.summingbird.planner._
import com.twitter.summingbird.viz.DagViz
import com.twitter.util.Future
import org.scalatest.WordSpec
import org.scalacheck._

import scala.collection.TraversableOnce
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

/**
 * Tests for Summingbird's Storm planner.
 */
object StormLaws {
  val outputList = new ArrayBuffer[Int] with SynchronizedBuffer[Int]

  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  val testFn = sample[Int => List[(Int, Int)]]

  implicit val storm = Storm.local(Map())

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def genStore: (String, Storm#Store[Int, Int]) = TestStore.createStore[Int, Int]()

  def genSink: () => ((Int) => Future[Unit]) = () => { x: Int =>
    append(x)
    Future.Unit
  }

  def memoryPlanWithoutSummer(original: List[Int])(mkJob: (Producer[Memory, Int], Memory#Sink[Int]) => TailProducer[Memory, Int]): List[Int] = {
    val memory = new Memory
    val outputList = ArrayBuffer[Int]()
    val sink: (Int) => Unit = { x: Int => outputList += x }

    val job = mkJob(
      Memory.toSource(original),
      sink
    )
    val topo = memory.plan(job)
    memory.run(topo)
    outputList.toList
  }

  def append(x: Int): Unit = {
    StormLaws.outputList += x
  }

  def runWithOutSummer(original: List[Int])(mkJob: (Producer[Storm, Int], Storm#Sink[Int]) => TailProducer[Storm, Int]): List[Int] = {
    val cluster = new LocalCluster()

    val job = mkJob(
      Storm.source(TraversableSpout(original)),
      Storm.sink[Int]({ (x: Int) => append(x); Future.Unit })
    )

    StormTestRun(job)
    StormLaws.outputList.toList
  }

  val nextFn = { pair: ((Int, (Int, Option[Int]))) =>
    val (k, (v, joinedV)) = pair
    List((k -> joinedV.getOrElse(10)))
  }

  val nextFn1 = { pair: ((Int, Option[Int])) =>
    val (v, joinedV) = pair
    List((joinedV.getOrElse(10)))
  }

  val serviceFn = sample[Int => Option[Int]]
  val service = ReadableServiceFactory[Int, Int](() => ReadableStore.fromFn(serviceFn))

}

// ALL TESTS START GO IN THE CLASS NOT OBJECT

class StormLaws extends WordSpec {
  import StormLaws._
  import MapAlgebra.sparseEquiv

  "StormPlatform matches Scala for single step jobs" in {
    val original = sample[List[Int]]
    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_, _)(testFn)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.singleStepInScala(original)(testFn),
      returnedState.toScala
    )
  }

  "FlatMap to nothing" in {
    val original = sample[List[Int]]
    val fn = { (x: Int) => List[(Int, Int)]() }
    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_, _)(fn)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.singleStepInScala(original)(fn),
      returnedState.toScala
    )
  }

  "OptionMap and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = sample[Int => Option[Int]]
    val fnB = sample[Int => List[(Int, Int)]]

    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.twinStepOptionMapFlatMapJob[Storm, Int, Int, Int, Int](_, _)(fnA, fnB)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.twinStepOptionMapFlatMapScala(original)(fnA, fnB),
      returnedState.toScala
    )
  }

  "OptionMap to nothing and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = { (x: Int) => None }
    val fnB = sample[Int => List[(Int, Int)]]

    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.twinStepOptionMapFlatMapJob[Storm, Int, Int, Int, Int](_, _)(fnA, fnB)
      )
    assertEquiv[Map[Int, Int]](
      TestGraphs.twinStepOptionMapFlatMapScala(original)(fnA, fnB),
      returnedState.toScala
    )
  }

  "StormPlatform matches Scala for large expansion single step jobs" in {
    val original = sample[List[Int]]
    val expander = sample[Int => List[(Int, Int)]]
    val expansionFunc = { (x: Int) =>
      expander(x).flatMap { case (k, v) => List((k, v), (k, v), (k, v), (k, v), (k, v)) }
    }
    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_, _)(expansionFunc)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.singleStepInScala(original)(expansionFunc),
      returnedState.toScala
    )
  }

  "StormPlatform matches Scala for flatmap keys jobs" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41) // sample[List[Int]]
    val fnA = sample[Int => List[(Int, Int)]]
    val fnB = sample[Int => List[Int]]
    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.singleStepMapKeysJob[Storm, Int, Int, Int, Int](_, _)(fnA, fnB)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB),
      returnedState.toScala
    )
  }

  "StormPlatform matches Scala for left join jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i -> i)) }
    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.leftJoinJob[Storm, Int, Int, Int, Int, Int](_, service, _)(staticFunc)(nextFn)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.leftJoinInScala(original)(serviceFn)(staticFunc)(nextFn),
      returnedState.toScala
    )
  }

  "StormPlatform matches Scala for left join with flatMapValues jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i -> i)) }

    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.leftJoinJobWithFlatMapValues[Storm, Int, Int, Int, Int, Int](_, service, _)(staticFunc)(nextFn1)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.leftJoinWithFlatMapValuesInScala(original)(serviceFn)(staticFunc)(nextFn1),
      returnedState.toScala
    )
  }

  "StormPlatform matches Scala for repeated tuple leftJoin jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i -> i)) }
    val returnedState =
      StormTestRun.simpleRun[Int, Int, Int](original,
        TestGraphs.repeatedTupleLeftJoinJob[Storm, Int, Int, Int, Int, Int](_, service, _)(staticFunc)(nextFn)
      )

    assertEquiv[Map[Int, Int]](
      TestGraphs.repeatedTupleLeftJoinInScala(original)(serviceFn)(staticFunc)(nextFn),
      returnedState.toScala
    )
  }

  "StormPlatform matches Scala for optionMap only jobs" in {
    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](createSource: SourceCreator[P], createStore: StoreCreator[P]): TailProducer[P, Any] =
        Source[P, Int](createSource("source"))
          .filter(_ % 2 == 0)
          .map(_ -> 10)
          .sumByKey(createStore("store"))
    })
  }

  "StormPlatform matches Scala for MapOnly/NoSummer" in {
    val original = sample[List[Int]]
    val doubler = { x: Int => List(x * 2) }

    val stormOutputList =
      runWithOutSummer(original)(
        TestGraphs.mapOnlyJob[Storm, Int, Int](_, _)(doubler)
      ).sorted

    val memoryOutputList =
      memoryPlanWithoutSummer(original)(TestGraphs.mapOnlyJob[Memory, Int, Int](_, _)(doubler)).sorted

    assert(stormOutputList == memoryOutputList)
  }

  "StormPlatform with multiple summers" in {
    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](createSource: SourceCreator[P], createStore: StoreCreator[P]): TailProducer[P, Any] = {
        TestGraphs.multipleSummerJob[P, Int, Int, Int, Int, Int, Int](
          Source[P, Int](createSource[Int]("source")),
          createStore("store1"),
          createStore("store2")
        )(x => List(x * 10), x => List((x, x)), x => List((x, x)))
      }
    })
  }

  "StormPlatform should be efficent in real world job" in {
    val original1 = sample[List[Int]]
    val original2 = sample[List[Int]]
    val original3 = sample[List[Int]]
    val original4 = sample[List[Int]]
    val source1 = Storm.source(TraversableSpout(original1))
    val source2 = Storm.source(TraversableSpout(original2))
    val source3 = Storm.source(TraversableSpout(original3))
    val source4 = Storm.source(TraversableSpout(original4))

    val fn1 = sample[(Int) => List[(Int, Int)]]
    val fn2 = sample[(Int) => List[(Int, Int)]]
    val fn3 = sample[(Int) => List[(Int, Int)]]

    val (store1Id, store1) = genStore

    val preJoinFn = sample[(Int) => (Int, Int)]
    val postJoinFn = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]

    val serviceFn = sample[Int => Option[Int]]
    val service = ReadableServiceFactory[Int, Int](() => ReadableStore.fromFn(serviceFn))

    val tail = TestGraphs.realJoinTestJob[Storm, Int, Int, Int, Int, Int, Int, Int, Int, Int](source1, source2, source3, source4,
      service, store1, fn1, fn2, fn3, preJoinFn, postJoinFn)

    assert(OnlinePlan(tail).nodes.size < 10)
    StormTestRun(tail)

    val scalaA = TestGraphs.realJoinTestJobInScala(original1, original2, original3, original4,
      serviceFn, fn1, fn2, fn3, preJoinFn, postJoinFn)

    val store1Map = TestStore[Int, Int](store1Id).get.toScala
    assertEquiv[Map[Int, Int]](
      scalaA,
      store1Map
    )
  }

  "StormPlatform should be able to handle AlsoProducer with Summer and FlatMap in different branches" in {
    val branchFlatMap = sample[((Int, Int)) => List[(Int, Int)]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](createSource: SourceCreator[P], createStore: StoreCreator[P]): TailProducer[P, Any] = {
        val source = Source[P, (Int, Int)](createSource("source"))
        source.sumByKey(createStore("store1")).also(
          source.flatMap(branchFlatMap).sumByKey(createStore("store2"))
        )
      }
    })

//    Fails, see https://github.com/twitter/summingbird/issues/725 for details.
//    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
//      override def apply[P <: Platform[P]](createSource: SourceCreator[P], createStore: StoreCreator[P]): TailProducer[P, Any] = {
//        val source = Source[P, (Int, Int)](createSource("source")).flatMap(e => List(e))
//        source.sumByKey(createStore("store1")).also(
//          source.flatMap(branchFlatMap).sumByKey(createStore("store2"))
//        )
//      }
//    })

//    Workaround
    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](createSource: SourceCreator[P], createStore: StoreCreator[P]): TailProducer[P, Any] = {
        val source = Source[P, (Int, Int)](createSource("source")).flatMap(e => List(e))
        source.map(identity).sumByKey(createStore("store1")).also(
          source.flatMap(branchFlatMap).sumByKey(createStore("store2"))
        )
      }
    })

//    Also fails.
//    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
//      override def apply[P <: Platform[P]](createSource: SourceCreator[P], createStore: StoreCreator[P]): TailProducer[P, Any] = {
//        val source = Source[P, (Int, Int)](createSource("source"))
//          .sumByKey(createStore("tmpStore")).map({ case (key, (_, value)) => (key, value) })
//
//        source.sumByKey(createStore("store1")).also(
//          source.flatMap(branchFlatMap).sumByKey(createStore("store2"))
//        )
//      }
//    })
  }

  def assertEquiv[T](expected: T, returned: T)(implicit equiv: Equiv[T]): Unit = {
    assert(equiv.equiv(expected, returned), (expected.toString, returned.toString))
  }
}
