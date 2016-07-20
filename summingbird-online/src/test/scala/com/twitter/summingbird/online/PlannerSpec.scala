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

package com.twitter.summingbird.online

import com.twitter.algebird.{ MapAlgebra, Semigroup }
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.memory._
import com.twitter.summingbird.planner._
import com.twitter.summingbird.online.option._
import com.twitter.util.Future
import org.scalatest.WordSpec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MMap }
import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._
import scala.util.{ Try, Success, Failure }

/**
 * Tests for Summingbird's Storm planner.
 */

class PlannerSpec extends WordSpec {
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  private type MemoryDag = Dag[Memory]
  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  import TestGraphGenerators._

  implicit def sink1: Memory#Sink[Int] = sample[Int => Unit]
  implicit def sink2: Memory#Sink[(Int, Int)] = sample[((Int, Int)) => Unit]

  implicit def testStore: Memory#Store[Int, Int] = MMap[Int, Int]()

  implicit val arbIntSource: Arbitrary[Producer[Memory, Int]] =
    Arbitrary(Gen.listOfN(100, Arbitrary.arbitrary[Int]).map {
      x: List[Int] =>
        Memory.toSource(x)
    })
  implicit val arbTupleSource: Arbitrary[KeyedProducer[Memory, Int, Int]] =
    Arbitrary(Gen.listOfN(100, Arbitrary.arbitrary[(Int, Int)]).map {
      x: List[(Int, Int)] =>
        IdentityKeyedProducer(Memory.toSource(x))
    })

  def arbSource1 = sample[Producer[Memory, Int]]
  def arbSource2 = sample[KeyedProducer[Memory, Int, Int]]

  /*
 * Test Case : A simple src.map.summer topology with FMMergeableWithSource opt-in functionality[ No FlatMappedProducer ].
 * Asserts : Online Plan takes options as an argument
 *           The plan considers the options to convert into a two node toplogy removing FlatMapNode.
 */
  "Basic Online Plan without a flat map and opt-in FMMergeableWithSource" in {
    val store1 = testStore
    val srcName = "first"
    val fmName = "mapped"
    val smName = "summer"
    val h1 = arbSource1.name("first")
      .map { i:Int => (i,i * i) }.name("mapped")
      .sumByKey(store1)

    val opts =  Map(
      fmName  -> Options().set(SummerParallelism(10)).set(FlatMapParallelism(50)).set(SourceParallelism(50)).set(FMMergeableWithSource(true)),
      smName  -> Options().set(SummerParallelism(20)),
      srcName -> Options().set(FlatMapParallelism(100)).set(SourceParallelism(100))
    )
    val planned = Try(OnlinePlan(h1,opts))
    planned match {
      case Success(graph) => {
        assert(true)
        assert(planned.get.nodes.size == 2)
      }
      case Failure(error) =>
        val path = TopologyPlannerLaws.dumpGraph(h1)
        error.printStackTrace
        println("Dumped failing graph for the basic online paln without flat map - writing to: " + path)
        assert(false)
    }

  }

  "The Online Plan with a flat Map which has no Summer as dependant" in {
    val store1 = testStore
   // val h1 = arbSource1.map{ i:Int => List(i+1,i+2,i+3) }.flatMap{ x => x.toTraversable }.map{ x => (x *2,1) }.sumByKey(store1)
    val h1 = arbSource1.flatMap{ i :Int => List((i+1,1),(i+2,1),(i+3,i))}.name("lol").sumByKey(store1)
    val planned = Try(OnlinePlan(h1))
    planned match {
      case Success(graph) => {
        assert(true == true)
      }
      case Failure(error) =>
        val path = TopologyPlannerLaws.dumpGraph(h1)
        error.printStackTrace
        println("Dumped failing graph for the flat map and no summer as dependant - writing to: " + path)
        assert(false)
    }
  }

  "Basic Online Plan with a flat map, before the summer" in {
    val store1 = testStore
    //val h1 = arbSource1.name("source of List(Ints)").flatMap { i:Int => List(i,i*2,i*3) }.map { x:Int => (x,1)}.sumByKey(store1)
    val h1 = arbSource1.name("source of List(Ints)").flatMap { i:Int => List((i,1),(i*2,1),(i*3,1)) }.sumByKey(store1)
    val planned = Try(OnlinePlan(h1))

    planned match {
      case Success(graph) => {
        assert(true == true)
      }
      case Failure(error) =>
        val path = TopologyPlannerLaws.dumpGraph(h1)
        error.printStackTrace
        println("Dumped failing graph for the basic plan with a flat map - writing to: " + path)
        assert(false)
    }
  }

  "Must be able to plan user supplied Job A" in {
    val store1 = testStore
    val store2 = testStore
    val store3 = testStore

    val h = arbSource1.name("name1")
      .flatMap { i: Int =>
        List(i, i)
      }
      .name("name1PostFM")
    val h2 = arbSource2.name("name2")
      .flatMap { tup: (Int, Int) =>
        List(tup._1, tup._2)
      }.name("name2PostFM")

    val combined = h2.merge(h)

    val s1 = combined.name("combinedPipes")
      .map { i: Int =>
        (i, i * 2)
      }

    val s2 = combined.map { i: Int =>
      (i, i * 3)
    }

    val tail = s1.sumByKey(store1)
      .name("Store one writter")
      .also(s2)
      .sumByKey(store2)

    val planned = Try(OnlinePlan(tail))


    planned match {
      case Success(graph) => {
        assert(true == true)
      }
      case Failure(error) =>
        val path = TopologyPlannerLaws.dumpGraph(tail)
        error.printStackTrace
        println("Dumped failing graph for ' Must be able to plan user supplied Job A ' to: " + path)
        assert(false)
    }
  }

  "Must be able to plan user supplied Job B" in {
    val store1 = testStore
    val store2 = testStore
    val store3 = testStore

    val h = arbSource1.name("name1")
      .flatMap { i: Int =>
        List(i, i)
      }
      .name("name1PostFM")
    val h2 = arbSource2.name("name2")
      .flatMap { tup: (Int, Int) =>
        List(tup._1, tup._2)
      }.name("name2PostFM")

    val combined = h2.merge(h)

    val s1 = combined.name("combinedPipes")
      .map { i: Int =>
        (i, i * 2)
      }

    val s2 = combined.map { i: Int =>
      (i, i * 3)
    }

    val s3 = combined.map { i: Int =>
      (i, i * 4)
    }

    val tail = s1.sumByKey(store1)
      .name("Store one writter")
      .also(s2)
      .sumByKey(store2)
      .name("Store two writer")
      .also(s3)
      .sumByKey(store3)
      .name("Store three writer")

    val planned = Try(OnlinePlan(tail))
    planned match {
      case Success(graph) => assert(true == true)
      case Failure(error) =>
        val path = TopologyPlannerLaws.dumpGraph(tail)
        error.printStackTrace
        println("Dumped failing graph to: " + path)
        assert(false)
    }
  }

  "Must be able to plan user supplied Job C" in {
    val store1 = testStore

    val src = arbSource1
    val h = src

    val h2 = src.map(_ * 3)

    val combined = h2.merge(h)

    val c1 = combined.map { i: Int => i * 4 }
    val c2 = combined.map { i: Int => i * 8 }
    val tail = c1.merge(c2).map { i: Int => (i, i) }.sumByKey(store1)

    val planned = Try(OnlinePlan(tail))
    planned match {
      case Success(graph) => assert(true == true)
      case Failure(error) =>
        val path = TopologyPlannerLaws.dumpGraph(tail)
        error.printStackTrace
        println("Dumped failing graph to: " + path)
        assert(false)
    }
  }
  "Chained SumByKey with extra Also is okay" in {
    val store1 = testStore
    val part1: TailProducer[Memory, (Int, (Option[Int], Int))] = arbSource1.map { i => (i % 10, i * i) }.sumByKey(store1).name("Sarnatsky")
    val store2 = testStore
    val part2 = part1.mapValues { case (optV, v) => v }
      .mapKeys(_ => 1).name("Preexpanded")
      .sumByKey(store2).name("All done")
    Try(OnlinePlan(part1.also(part2))) match {
      case Success(graph) =>
        TopologyPlannerLaws.dumpGraph(graph)
        TopologyPlannerLaws.dumpGraph(part2)
        assert(TopologyPlannerLaws.summersOnlyShareNoOps(graph) == true)
      case Failure(error) =>
        val path = TopologyPlannerLaws.dumpGraph(part2)
        error.printStackTrace
        println("Dumped failing graph to: " + path)
        assert(false)
    }
  }


}
