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

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.singleStepJob[P, Int, Int, Int](
          ctx.source(original), ctx.store[Int, Int]("store"))(fn)
    })
  }

  "FlatMap to nothing" in {
    val original = sample[List[Int]]
    val fn = { (x: Int) => List[(Int, Int)]() }

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.singleStepJob[P, Int, Int, Int](
          ctx.source(original), ctx.store[Int, Int]("store"))(fn)
    })
  }

  "OptionMap and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = sample[Int => Option[Int]]
    val fnB = sample[Int => List[(Int, Int)]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.twinStepOptionMapFlatMapJob[P, Int, Int, Int, Int](
          ctx.source(original), ctx.store[Int, Int]("store"))(fnA, fnB)
    })
  }

  "OptionMap to nothing and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = { (x: Int) => None }
    val fnB = sample[Int => List[(Int, Int)]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.twinStepOptionMapFlatMapJob[P, Int, Int, Int, Int](
          ctx.source(original), ctx.store[Int, Int]("store"))(fnA, fnB)
    })
  }

  "StormPlatform matches Scala for large expansion single step jobs" in {
    val original = sample[List[Int]]
    val expander = sample[Int => List[(Int, Int)]]
    val expansionFunc = { (x: Int) =>
      expander(x).flatMap { case (k, v) => List((k, v), (k, v), (k, v), (k, v), (k, v)) }
    }

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.singleStepJob[P, Int, Int, Int](
          ctx.source(original), ctx.store[Int, Int]("store"))(expansionFunc)
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
    val staticFunc = { i: Int => List((i, i)) }

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.leftJoinJob[P, Int, Int, Int, Int, Int](
          ctx.source(original), ctx.service(serviceFn), ctx.store("store"))(staticFunc)(nextFn)
    })
  }

  "StormPlatform matches Scala for left join with flatMapValues jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i, i)) }

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.leftJoinJobWithFlatMapValues[P, Int, Int, Int, Int, Int](
          ctx.source(original), ctx.service(serviceFn), ctx.store("store"))(staticFunc)(nextFn1)
    })
  }

  "StormPlatform matches Scala for repeated tuple leftJoin jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i, i)) }

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.repeatedTupleLeftJoinJob[P, Int, Int, Int, Int, Int](
          ctx.source(original), ctx.service(serviceFn), ctx.store("store"))(staticFunc)(nextFn)
    })
  }

  "StormPlatform matches Scala for optionMap only jobs" in {
    val original = sample[List[Int]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        ctx.source(original)
          .filter(_ % 2 == 0)
          .map(_ -> 10)
          .sumByKey(ctx.store("store"))
    })
  }

  "StormPlatform matches Scala for MapOnly/NoSummer" in {
    val original = sample[List[Int]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.mapOnlyJob[P, Int, Int](ctx.source(original), ctx.sink[Int]("sink"))(x => List(x))
    })
  }

  "StormPlatform with multiple summers" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 41) // sample[List[Int]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.multipleSummerJob[P, Int, Int, Int, Int, Int, Int](
          ctx.source(original),
          ctx.store("store1"),
          ctx.store("store2")
        )(x => List(x * 10), x => List((x, x)), x => List((x, x)))
    })
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

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] =
        TestGraphs.realJoinTestJob[P, Int, Int, Int, Int, Int, Int, Int, Int, Int](
          ctx.source(original1), ctx.source(original2), ctx.source(original3), ctx.source(original4),
          ctx.service(serviceFn), ctx.store("store"), fn1, fn2, fn3, preJoinFn, postJoinFn)
    })
  }

  "StormPlatform should be able to handle AlsoProducer with Summer and FlatMap in different branches" in {
    val original = sample[List[(Int, Int)]]
    val branchFlatMap = sample[((Int, Int)) => List[(Int, Int)]]

    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
        val source = ctx.source(original)
        source.sumByKey(ctx.store("store1")).also(
          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
        )
      }
    })

//    Fails, see https://github.com/twitter/summingbird/issues/725 for details.
//    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
//      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
//        val source = ctx.source(original).flatMap(e => List(e))
//        source.sumByKey(ctx.store("store1")).also(
//          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
//        )
//      }
//    })

//    Workaround
    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
        val source = ctx.source(original).flatMap(e => List(e))
        source.map(identity).sumByKey(ctx.store("store1")).also(
          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
        )
      }
    })

//    Also fails.
//    StormTestUtils.testStormEqualToMemory(new ProducerCreator {
//      override def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any] = {
//        val source = ctx.source(original)
//          .sumByKey(ctx.store("tmpStore")).map({ case (key, (_, value)) => (key, value) })
//
//        source.sumByKey(ctx.store("store1")).also(
//          source.flatMap(branchFlatMap).sumByKey(ctx.store("store2"))
//        )
//      }
//    })
  }
}
