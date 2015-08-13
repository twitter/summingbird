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

import backtype.storm.generated.StormTopology
import com.twitter.algebird.{ MapAlgebra, Semigroup }
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.online._
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.storm.option._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.tormenta.spout.Spout
import com.twitter.util.Future
import java.util.{ Collections, HashMap, Map => JMap, UUID }
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.WordSpec
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{
  ArrayBuffer,
  HashMap => MutableHashMap,
  Map => MutableMap,
  SynchronizedBuffer,
  SynchronizedMap
}
/**
 * Tests for Summingbird's Storm planner.
 */

class TopologyTests extends WordSpec {
  import MapAlgebra.sparseEquiv

  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  /**
   * The function tested below. We can't generate a function with
   * ScalaCheck, as we need to know the number of tuples that the
   * flatMap will produce.
   */
  val testFn = { i: Int => List((i -> i)) }

  implicit val storm = Storm.local()

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  /**
   * Perform a single run of TestGraphs.singleStepJob using the
   * supplied list of integers and the testFn defined above.
   */
  def funcToPlan(mkJob: (Producer[Storm, Int], Storm#Store[Int, Int]) => TailProducer[Storm, Any]): StormTopology = {
    val original = sample[List[Int]]

    val job = mkJob(
      Storm.source(TraversableSpout(original)),
      TestStore.createStore[Int, Int]()._2
    )

    storm.plan(job).topology
  }

  "Number of bolts should be as expected" in {
    val stormTopo =
      funcToPlan(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_, _)(testFn)
      )
    // Final Flatmap + summer
    assert(stormTopo.get_bolts_size() == 2)
  }

  "Number of spouts in simple task should be 1" in {
    val stormTopo =
      funcToPlan(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_, _)(testFn)
      )
    // Source producer
    assert(stormTopo.get_spouts_size() == 1)
  }

  "A named node after a flat map should imply its options" in {
    val nodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(nodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(nodeName -> Options().set(FlatMapParallelism(50)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(1).get_common.get_parallelism_hint == 50)
  }

  "With 2 names in a row we take the closest name" in {
    val nodeName = "super dooper node"
    val otherNodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(nodeName).name(otherNodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(otherNodeName -> Options().set(FlatMapParallelism(40)),
      nodeName -> Options().set(FlatMapParallelism(50)))

    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(1).get_common.get_parallelism_hint == 50)
  }

  "If the closes doesnt contain the option we keep going" in {
    val nodeName = "super dooper node"
    val otherNodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(otherNodeName).name(nodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(otherNodeName -> Options().set(SourceParallelism(30)),
      nodeName -> Options().set(FlatMapParallelism(50)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(1).get_common.get_parallelism_hint == 50)
  }

  "Options propagate backwards" in {
    val nodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(nodeName).name("Throw away name")
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(nodeName -> Options().set(FlatMapParallelism(50)).set(SourceParallelism(30)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts
    val spouts = stormTopo.get_spouts
    val spout = spouts.head._2

    assert(spout.get_common.get_parallelism_hint == 30)
  }

  "Options don't propagate forwards" in {
    val nodeName = "super dooper node"
    val otherNodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(otherNodeName).name(nodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(otherNodeName -> Options().set(SourceParallelism(30)).set(SummerParallelism(50)),
      nodeName -> Options().set(FlatMapParallelism(50)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(0).get_common.get_parallelism_hint == 5)
  }
}
