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

import backtype.storm.{ Config => BacktypeStormConfig, LocalCluster, Testing }
import backtype.storm.generated.StormTopology
import backtype.storm.testing.{ CompleteTopologyParam, MockedSources }
import com.twitter.algebird.{Monoid, MapAlgebra, Semigroup}
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.summingbird.option._
import com.twitter.summingbird.storm.option.{CacheSize => oCacheSize, _}
import com.twitter.summingbird.online.option.{FlushFrequency, UseAsyncCache, AsyncPoolSize}
import com.twitter.util.Duration
import com.twitter.summingbird.memory._
import com.twitter.summingbird.planner._
import com.twitter.tormenta.spout.Spout
import com.twitter.util.Future
import org.specs2.mutable._
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties
import java.util.{Collections, HashMap, Map => JMap, UUID}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.{
  ArrayBuffer,
  HashMap => MutableHashMap,
  Map => MutableMap,
  SynchronizedBuffer,
  SynchronizedMap
}
import com.twitter.summingbird.online.executor.InflightTuples
/**
  * Tests for Summingbird's Storm planner.
  */


object StormBenchmark extends Specification {
  import MapAlgebra.sparseEquiv

  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  /**
    * Global state shared by all tests.
    */
  val globalState = TrueGlobalState.data

  val testFn = sample[Int => List[(Int, Int)]]

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def genStore: (String, Storm#Store[Int, Int])  = {
    val id = UUID.randomUUID.toString
    globalState += (id -> TestState())
    val store = MergeableStoreSupplier(() => StormLaws.testingStore(id), Batcher.unit)
    (id, store)
  }

  object StormRunner {
    private def completeTopologyParam(conf: BacktypeStormConfig) = {
      val ret = new CompleteTopologyParam()
      ret.setMockedSources(new MockedSources)
      ret.setStormConf(conf)
      ret.setCleanupState(false)
      ret
    }

    def run(plannedTopology: PlannedTopology): Long = {
      val cluster = new LocalCluster()
      val start = System.currentTimeMillis
      Testing.completeTopology(cluster, plannedTopology.topology, completeTopologyParam(plannedTopology.config))
      val stop = System.currentTimeMillis
      // Sleep to prevent this race: https://github.com/nathanmarz/storm/pull/667
      Thread.sleep(1000)
      cluster.shutdown
      stop - start
    }
  }

  def getProducer = {
    val original = (1 until 2000).map(x => sample[Int])
    val expansionFunc = {(x: Int) =>
                  (1 until 120).map(x => (sample[Int], sample[Int]))
      }
    val (id, store) = genStore
    val src = Storm.source(TraversableSpout(original))
    src
      .flatMap(expansionFunc).name("FM")
      .sumByKey(store)
  }

  def runBenchmark(storm: Storm, n: Int): Double = {
    var timer = 0L
    (1 to n).foreach { runNum =>
      val topo = storm.plan(getProducer)
      timer += StormRunner.run(topo)
    }
    timer.toDouble / n
  }

  val configs = List(
    ("Cache 0, FlushFrequency 1:, Async Disabled",
    Map(
      "DEFAULT" -> Options().set(AnchorTuples(true)),
      "FM" -> Options().set(CacheSize(0)).set(FlushFrequency(Duration.fromMilliseconds(1)))
      )
    ),
    ("Cache 10, FlushFrequency 40:, Async Disabled",
    Map(
      "DEFAULT" -> Options().set(AnchorTuples(true)),
      "FM" -> Options().set(CacheSize(10)).set(FlushFrequency(Duration.fromMilliseconds(40)))
      )
    ),
    ("Cache 100, FlushFrequency 100:, Async Disabled",
    Map(
      "DEFAULT" -> Options().set(AnchorTuples(true)),
      "FM" -> Options().set(CacheSize(100)).set(FlushFrequency(Duration.fromMilliseconds(100)))
      )
    ),
    ("Cache 100, FlushFrequency 100:, Async Enabled, Async Pool Size: 0",
    Map(
      "DEFAULT" -> Options().set(AnchorTuples(true)),
      "FM" -> Options().set(CacheSize(100)).set(FlushFrequency(Duration.fromMilliseconds(100))).set(UseAsyncCache(true)).set(AsyncPoolSize(0))
      )
    ),
    ("Cache 100, FlushFrequency 100:, Async Enabled, Async Pool Size: 5",
    Map(
      "DEFAULT" -> Options().set(AnchorTuples(true)),
      "FM" -> Options().set(CacheSize(100)).set(FlushFrequency(Duration.fromMilliseconds(100))).set(UseAsyncCache(true)).set(AsyncPoolSize(5))
      )
    ),
    ("Cache 100, FlushFrequency 5:, Async Enabled, Async Pool Size: 5",
    Map(
      "DEFAULT" -> Options().set(AnchorTuples(true)),
      "FM" -> Options().set(CacheSize(100)).set(FlushFrequency(Duration.fromMilliseconds(5))).set(UseAsyncCache(true)).set(AsyncPoolSize(5))
      )
    ),
    ("Cache 10, FlushFrequency 0:, Async Enabled, Async Pool Size: 5",
    Map(
      "DEFAULT" -> Options().set(AnchorTuples(true)),
      "FM" -> Options().set(CacheSize(10)).set(FlushFrequency(Duration.fromMilliseconds(0))).set(UseAsyncCache(true)).set(AsyncPoolSize(5))
      )
    )
  )

  def run(n: Int, cfg: (String, Map[String, Options])): Double = {
    InflightTuples.reset
    val (settings, sbConfig) = cfg
    println("Starting(" + settings + ")")
    val avgTime = runBenchmark(Storm.local(sbConfig), n)
    println("Average time (" + settings + ") is :" + avgTime/1000 + " seconds")
    InflightTuples.query must be_==(0)
    avgTime
  }

  // "Benchmark Runs" in {
  //   configs.foreach{ cfg =>
  //     run(5, cfg)
  //   }
  // }
}
