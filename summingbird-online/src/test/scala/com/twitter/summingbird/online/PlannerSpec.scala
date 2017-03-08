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

import com.twitter.summingbird._
import com.twitter.summingbird.memory._
import com.twitter.summingbird.planner._
import com.twitter.summingbird.online.option._
import org.scalatest.WordSpec
import scala.collection.mutable.{ Map => MMap }
import org.scalacheck._
import Arbitrary._
import scala.util.{ Failure, Success, Try }

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
 * Test Case : A simple src.map.summer topology with FMMergeableWithSource opt-in functionality.
 * Asserts : The plan considers the options to convert into a two node toplogy removing FlatMapNode.
 */

  "The Online Plan with a flat Map which has Summer as dependant and opted-in for FMMergeableWithSource" in {
    val store1 = testStore
    val fmname = "flatmapped"
    val h1 = arbSource1
      .flatMap { i: Int => List((i + 1, 1), (i + 2, 1), (i + 3, i)) }.name(fmname)
      .sumByKey(store1)

    val opts = Map(
      fmname -> Options().set(FMMergeableWithSource(true))
    )
    val planned = Try(OnlinePlan(h1, opts))

    assert(planned.isSuccess, "FAILED : The Online Plan with a flat Map which has no Summer as dependant - writing to: " + TopologyPlannerLaws.dumpGraph(h1))
    assert(planned.get.nodes.size == 2)
  }

  /*
   * Tests the fanOut case on the FlatMappedProducer when opt-in to FlatMapNode mergeable with SourceNode
   * Asserts : SourceNode has two FlatMapNodes
   *           Each FlatMapNode has a SummerNode as dependant.
   */
  "FMMergeableWithSource with a fanOut case after flatMap" in {

    def testStore2: Memory#Store[Int, Int] = MMap[Int, Int]()
    val sumName = "summer"

    val p1 = arbSource1.flatMap { i: Int => List((i -> i)) }
    val p2 = p1.sumByKey(testStore).name("sum1")
    val p3 = p1.map { x => x }.sumByKey(testStore2).name("sum2")
    val p = p2.also(p3)

    val opts = Map("sum1" -> Options().set(FMMergeableWithSource(true)).set(FlatMapParallelism(15)),
      "sum2" -> Options().set(SourceParallelism(50)).set(FMMergeableWithSource(true)))

    val storm = Try(OnlinePlan(p, opts))

    // Source Node should exist and have two FlatMapNodes as dependants
    val sourceNodes = storm.get.dependantsOfM.keys.find(_.toString contains "SourceNode")
    sourceNodes match {
      case Some(s) => storm.get.dependantsOfM.get(s) match {
        case Some(listOfFlatMapNodes) => assert(listOfFlatMapNodes.size == 2)
        case None => assert(false, "No FlatMapNodes are found in dependant list of Source where as two FlatMapNodes are expected.")
      }
      case None => assert(false, "Could not find a SourceNode.")
    }

    // Each FlatMapNode should have a SummerNode as dependant.
    val flatMapnodes = storm.get.dependantsOfM.filterKeys { _.toString contains "FlatMapNode" }
    val fmnValues = flatMapnodes.values
    fmnValues.foreach {
      x: List[Node[Memory]] =>
        assert(x.size == 1)
        assert(x(0).toString contains "SummerNode")
    }
  }

  /*
   * Tests the alsoProducer case for two seperate sub-topologies.
   * Asserts : There are two sourceNodes
   *           Each SourceNode has a single dependant.
   *           Each SourceNode has a summer as dependant.
   */
  "Also producer with two topos" in {
    def testStore2: Memory#Store[Int, Int] = MMap[Int, Int]()

    val p1 = arbSource1.flatMap { i: Int => List((i -> i)) }.sumByKey(testStore).name("topo1")
    val p2 = arbSource2.flatMap { tup: (Int, Int) => List((tup._1, tup._2)) }.sumByKey(testStore2).name("topo2")
    val p = p1.also(p2)

    val opts = Map("topo1" -> Options().set(FMMergeableWithSource(true)),
      "topo2" -> Options().set(FMMergeableWithSource(true)))
    val storm = Try(OnlinePlan(p, opts))
    val dependents = storm.get.dependantsOfM
    val srcNodes = dependents.filterKeys { _.toString contains "SourceNode" }
    assert(srcNodes.keySet.size == 2)
    srcNodes.keySet.foreach { x =>
      val dependant = dependents.get(x)
      assert(dependant.size == 1)
      assert(dependant.head.toString contains "SummerNode")
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
        assert(true)
      }
      case Failure(error) =>
        error.printStackTrace
        fail("Dumped failing graph for ' Must be able to plan user supplied Job A ' to: " + TopologyPlannerLaws.dumpGraph(tail))
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
        fail("Dumped failing graph to: " + path)
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
        fail("Dumped failing graph to: " + path)
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
        fail("Dumped failing graph to: " + path)
    }
  }
}
