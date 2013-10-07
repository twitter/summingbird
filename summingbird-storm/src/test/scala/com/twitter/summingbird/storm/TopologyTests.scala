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

import backtype.storm.{ LocalCluster, Testing }
import backtype.storm.generated.StormTopology
import backtype.storm.testing.{ CompleteTopologyParam, MockedSources }
import com.twitter.algebird.{MapAlgebra, Monoid}
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.storm.option._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.tormenta.spout.Spout
import com.twitter.util.Future
import java.util.{Collections, HashMap, Map => JMap, UUID}
import java.util.concurrent.atomic.AtomicInteger
import org.specs._
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


object ToplogyTests extends Specification {
  import MapAlgebra.sparseEquiv

  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  def createGlobalState[T, K, V] =
    new MutableHashMap[String, TestState[T, K, V]]
        with SynchronizedMap[String, TestState[T, K, V]]

  /**
    * Global state shared by all tests.
    */
  val globalState = createGlobalState[Int, Int, Int]

  /**
    * Returns a MergeableStore that routes get, put and merge calls
    * through to the backing store in the proper globalState entry.
    */
  def testingStore(id: String) =
    new MergeableStore[(Int, BatchID), Int] with java.io.Serializable {
      val monoid = implicitly[Monoid[Int]]
      def wrappedStore = globalState(id).store
      private def getOpt(k: (Int, BatchID)) = Option(wrappedStore.get(k)).flatMap(i => i)
      override def get(k: (Int, BatchID)) = Future.value(getOpt(k))
      override def put(pair: ((Int, BatchID), Option[Int])) = {
        val (k, optV) = pair
        if (optV.isDefined)
          wrappedStore.put(k, optV)
        else
          wrappedStore.remove(k)
        globalState(id).placed.incrementAndGet
        Future.Unit
      }
      override def merge(pair: ((Int, BatchID), Int)) = {
        val (k, v) = pair
        val newV = Monoid.plus(Some(v), getOpt(k)).flatMap(Monoid.nonZeroOption(_))
        wrappedStore.put(k, newV)
        globalState(id).placed.incrementAndGet
        Future.Unit
      }
    }

  /**
    * The function tested below. We can't generate a function with
    * ScalaCheck, as we need to know the number of tuples that the
    * flatMap will produce.
    */
  val testFn = { i: Int => List((i -> i)) }

  val storm = Storm.local()

  val completeTopologyParam = {
    val ret = new CompleteTopologyParam()
    ret.setMockedSources(new MockedSources)
    ret.setStormConf(storm.baseConfig)
    ret.setCleanupState(false)
    ret
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  /**
    * Perform a single run of TestGraphs.singleStepJob using the
    * supplied list of integers and the testFn defined above.
    */
  def funcToPlan(mkJob: (Producer[Storm, Int], Storm#Store[Int, Int]) => Summer[Storm, Int, Int])
      : StormTopology = {
    val original = sample[List[Int]]
    val id = UUID.randomUUID.toString
    globalState += (id -> TestState())

    val job = mkJob(
      Storm.source(TraversableSpout(original)),
      MergeableStoreSupplier(() => testingStore(id), Batcher.unit)
    )

    storm.plan(job)
  }

  "Number of bolts should be as expected" in {
    val stormTopo =
      funcToPlan(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_,_)(testFn)
      )
    // Final Flatmap + summer
    stormTopo.get_bolts_size() must be(2)
  }

  "Number of spouts in simple task should be 1" in {
    val stormTopo =
      funcToPlan(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_,_)(testFn)
      )
    // Source producer
    stormTopo.get_spouts_size() must be(1)
  }

  "A named node after a flat map should imply its options" in {
  	val nodeName = "super dooper node"
  	val p = Storm.source(TraversableSpout(sample[List[Int]]))
  		.flatMap(testFn).name(nodeName)
      .sumByKey(MergeableStoreSupplier(() => testingStore(UUID.randomUUID.toString), Batcher.unit))
      
  	val opts = Map(nodeName -> Options().set(FlatMapParallelism(50)))
  	val storm = Storm.local(opts)  			
  	val stormTopo = storm.plan(p)
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map{case (k, v) => (k.split("-").size - 1, v)}
    
	TDistMap(1).get_common.get_parallelism_hint must be(50)
  }

  "With 2 names in a row we take the closest name" in {
  	val nodeName = "super dooper node"
  	val p = Storm.source(TraversableSpout(sample[List[Int]]))
  		.flatMap(testFn).name(nodeName).name("Throw away name")
      .sumByKey(MergeableStoreSupplier(() => testingStore(UUID.randomUUID.toString), Batcher.unit))
      
  	val opts = Map(nodeName -> Options().set(FlatMapParallelism(50)))
  	val storm = Storm.local(opts)  			
  	val stormTopo = storm.plan(p)
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map{case (k, v) => (k.split("-").size - 1, v)}
    
	TDistMap(1).get_common.get_parallelism_hint must be(50)
  }

  "Options propagate backwards" in {
  	val nodeName = "super dooper node"
  	val p = Storm.source(TraversableSpout(sample[List[Int]]))
  		.flatMap(testFn).name(nodeName).name("Throw away name")
      .sumByKey(MergeableStoreSupplier(() => testingStore(UUID.randomUUID.toString), Batcher.unit))
      
  	val opts = Map(nodeName -> Options().set(FlatMapParallelism(50)).set(SpoutParallelism(30)))
  	val storm = Storm.local(opts)  			
  	val stormTopo = storm.plan(p)
    // Source producer
    val bolts = stormTopo.get_bolts
    val spouts = stormTopo.get_spouts
    val spout = spouts.head._2
    
	spout.get_common.get_parallelism_hint must be(30)
  }
}
