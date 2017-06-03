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

import com.twitter.algebird.MapAlgebra
import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.summingbird.online._
import com.twitter.summingbird.planner._
import org.scalatest.WordSpec
import org.scalacheck._

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

  def genStore: (String, Storm#Store[Int, Int]) = TestStore.createStore[Int, Int]()

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
    val fn = sample[Int => List[(Int, Int)]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.singleStepJob[P, Int, Int, Int](
          ctx.source[Int]("source"), ctx.store[Int, Int]("store"))(fn)
    })
  }

  "FlatMap to nothing" in {
    val fn = { (x: Int) => List[(Int, Int)]() }

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.singleStepJob[P, Int, Int, Int](
          ctx.source[Int]("source"), ctx.store[Int, Int]("store"))(fn)
    })
  }

  "OptionMap and FlatMap" in {
    val fnA = sample[Int => Option[Int]]
    val fnB = sample[Int => List[(Int, Int)]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.twinStepOptionMapFlatMapJob[P, Int, Int, Int, Int](
          ctx.source[Int]("source"), ctx.store[Int, Int]("store"))(fnA, fnB)
    })
  }

  "OptionMap to nothing and FlatMap" in {
    val fnA = { (x: Int) => None }
    val fnB = sample[Int => List[(Int, Int)]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.twinStepOptionMapFlatMapJob[P, Int, Int, Int, Int](
          ctx.source[Int]("source"), ctx.store[Int, Int]("store"))(fnA, fnB)
    })
  }

  "StormPlatform matches Scala for large expansion single step jobs" in {
    val expander = sample[Int => List[(Int, Int)]]
    val expansionFunc = { (x: Int) =>
      expander(x).flatMap { case (k, v) => List((k, v), (k, v), (k, v), (k, v), (k, v)) }
    }

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.singleStepJob[P, Int, Int, Int](
          ctx.source[Int]("source"), ctx.store[Int, Int]("store"))(expansionFunc)
    })
  }

  "StormPlatform matches Scala for flatmap keys jobs" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41) // sample[List[Int]]
    val fnA = sample[Int => List[(Int, Int)]]
    val fnB = sample[Int => List[Int]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.singleStepMapKeysJob[P, Int, Int, Int, Int](
          ctx.source(original), ctx.store[Int, Int]("store"))(fnA, fnB)
    })
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
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        ctx.source[Int]("source")
          .filter(_ % 2 == 0)
          .map(_ -> 10)
          .sumByKey(ctx.store("store"))
    })
  }

  "StormPlatform matches Scala for MapOnly/NoSummer" in {
    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.mapOnlyJob[P, Int, Int](ctx.source[Int]("source"), ctx.sink[Int]("sink"))(x => List(x))
    })
  }

  "StormPlatform with multiple summers" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41) // sample[List[Int]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
        TestGraphs.multipleSummerJob[P, Int, Int, Int, Int, Int, Int](
          ctx.source(original),
          ctx.store("store1"),
          ctx.store("store2")
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
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
        val source = ctx.source[(Int, Int)]("source")
        source.sumByKey(ctx.store("store1")).also(
          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
        )
      }
    })

//    Fails, see https://github.com/twitter/summingbird/issues/725 for details.
//    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
//      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
//        val source = ctx.source[(Int, Int)]("source").flatMap(e => List(e))
//        source.sumByKey(ctx.store("store1")).also(
//          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
//        )
//      }
//    })

//    Workaround
    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
        val source = ctx.source[(Int, Int)]("source").flatMap(e => List(e))
        source.map(identity).sumByKey(ctx.store("store1")).also(
          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
        )
      }
    })

//    Also fails.
//    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
//      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
//        val source = ctx.source[(Int, Int)]("source")
//          .sumByKey(ctx.store("tmpStore")).map({ case (key, (_, value)) => (key, value) })
//
//        source.sumByKey(ctx.store("store1")).also(
//          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
//        )
//      }
//    })
  }

  def assertEquiv[T](expected: T, returned: T)(implicit equiv: Equiv[T]): Unit = {
    assert(equiv.equiv(expected, returned), (expected.toString, returned.toString))
  }
}
