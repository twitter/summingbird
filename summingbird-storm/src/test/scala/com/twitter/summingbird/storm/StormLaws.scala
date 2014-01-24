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
import com.twitter.algebird.{MapAlgebra, Semigroup}
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.summingbird.storm.option._
import com.twitter.summingbird.memory._
import com.twitter.summingbird.planner._
import com.twitter.tormenta.spout.Spout
import com.twitter.util.Future
import java.util.{Collections, HashMap, Map => JMap, UUID}
import java.util.concurrent.atomic.AtomicInteger
import org.specs2.mutable._
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable.{
  ArrayBuffer,
  HashMap => MutableHashMap,
  Map => MutableMap,
  SynchronizedBuffer,
  SynchronizedMap
}
import java.security.Permission
/**
  * Tests for Summingbird's Storm planner.
  */

/**
  * State required to perform a single Storm test run.
  */
case class TestState[T, K, V](
  store: JMap[(K, BatchID), Option[V]] = Collections.synchronizedMap(new HashMap[(K, BatchID), Option[V]]()),
  used: ArrayBuffer[T] = new ArrayBuffer[T] with SynchronizedBuffer[T],
  placed: AtomicInteger = new AtomicInteger
)

object TrueGlobalState {
  val data = new MutableHashMap[String, TestState[Int, Int, Int]]
        with SynchronizedMap[String, TestState[Int, Int, Int]]
}

  class MySecurityManager extends SecurityManager {
	  override def checkExit(status: Int): Unit = {
	    throw new SecurityException();
	  }
	  override def checkAccess(t: Thread) = {}
	  override def checkPermission(p: Permission) = {}
  }

/*
 * This is a wrapper to run a storm topology.
 * We use the SecurityManager code to catch the System.exit storm calls when it
 * fails. We wrap it into a normal exception instead so it can report better/retry.
 */

object StormRunner {
  private def completeTopologyParam(conf: BacktypeStormConfig) = {
    val ret = new CompleteTopologyParam()
    ret.setMockedSources(new MockedSources)
    ret.setStormConf(conf)
    ret.setCleanupState(false)
    ret
  }

  private def tryRun(plannedTopology: PlannedTopology): Unit = {
    //Before running the external Command
    val oldSecManager = System.getSecurityManager()
    System.setSecurityManager(new MySecurityManager());
    try {
	    val cluster = new LocalCluster()
	    Testing.completeTopology(cluster, plannedTopology.topology, completeTopologyParam(plannedTopology.config))
	    // Sleep to prevent this race: https://github.com/nathanmarz/storm/pull/667
	    Thread.sleep(1000)
	    cluster.shutdown
    } finally {
      System.setSecurityManager(oldSecManager)
    }
  }

  def run(plannedTopology: PlannedTopology) {
    this.synchronized {
       try {
        tryRun(plannedTopology)
      } catch {
        case _: Throwable =>
          Thread.sleep(3000)
          tryRun(plannedTopology)
      }
    }
  }
}

object StormLaws extends Specification {
  sequential
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

  /**
    * Returns a MergeableStore that routes get, put and merge calls
    * through to the backing store in the proper globalState entry.
    */
  def testingStore(id: String) =
    new MergeableStore[(Int, BatchID), Int] with java.io.Serializable {
      val semigroup = implicitly[Semigroup[Int]]
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
        val oldV = getOpt(k)
        val newV = Semigroup.plus(Some(v), oldV)
        wrappedStore.put(k, newV)
        globalState(id).placed.incrementAndGet
        Future.value(oldV)
      }
    }

  val testFn = sample[Int => List[(Int, Int)]]

  val storm = Storm.local(Map(
      ))

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def genStore: (String, Storm#Store[Int, Int])  = {
    val id = UUID.randomUUID.toString
    globalState += (id -> TestState())
    val store = MergeableStoreSupplier(() => testingStore(id), Batcher.unit)
    (id, store)
  }

  def genSink:() => ((Int) => Future[Unit]) = () => {x: Int =>
      append(x)
      Future.Unit
    }

  /**
    * Perform a single run of TestGraphs.singleStepJob using the
    * supplied list of integers and the testFn defined above.
    */
  def runOnce(original: List[Int])(mkJob: (Producer[Storm, Int], Storm#Store[Int, Int]) => TailProducer[Storm, Any])
      : TestState[Int, Int, Int] = {

	  val (id, store) = genStore

    val job = mkJob(
      Storm.source(TraversableSpout(original)),
      store
    )
    val topo = storm.plan(job)
    StormRunner.run(topo)
    globalState(id)
  }

  def memoryPlanWithoutSummer(original: List[Int])(mkJob: (Producer[Memory, Int], Memory#Sink[Int]) => TailProducer[Memory, Int])
  : List[Int] = {
    val memory = new Memory
    val outputList = ArrayBuffer[Int]()
    val sink: (Int) => Unit = {x: Int => outputList += x}

    val job = mkJob(
      Memory.toSource(original),
      sink
    )
    val topo = memory.plan(job)
    memory.run(topo)
    outputList.toList
  }

  val outputList = new ArrayBuffer[Int] with SynchronizedBuffer[Int]

  def append(x: Int):Unit = {
    StormLaws.outputList += x
  }

  def runWithOutSummer(original: List[Int])(mkJob: (Producer[Storm, Int], Storm#Sink[Int]) => TailProducer[Storm, Int])
      : List[Int] = {
    val cluster = new LocalCluster()

    val job = mkJob(
      Storm.source(TraversableSpout(original)),
      Storm.sink[Int]({ (x: Int) => append(x); Future.Unit })
    )

    val topo = storm.plan(job)
    StormRunner.run(topo)
    StormLaws.outputList.toList
  }



  val nextFn = { pair: ((Int, (Int, Option[Int]))) =>
    val (k, (v, joinedV)) = pair
    List((k -> joinedV.getOrElse(10)))
  }

  val serviceFn = sample[Int => Option[Int]]
  val service = StoreWrapper[Int, Int](() => ReadableStore.fromFn(serviceFn))

  // ALL TESTS START AFTER THIS LINE

  "StormPlatform matches Scala for single step jobs" in {
    val original = sample[List[Int]]
    val returnedState =
      runOnce(original)(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_,_)(testFn)
      )


    Equiv[Map[Int, Int]].equiv(
      TestGraphs.singleStepInScala(original)(testFn),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

  "FlatMap to nothing" in {
    val original = sample[List[Int]]
    val fn = {(x: Int) => List[(Int, Int)]()}
    val returnedState =
      runOnce(original)(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_,_)(fn)
      )


    Equiv[Map[Int, Int]].equiv(
      TestGraphs.singleStepInScala(original)(fn),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

  "OptionMap and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = sample[Int => Option[Int]]
    val fnB = sample[Int => List[(Int,Int)]]

    val returnedState =
      runOnce(original)(
        TestGraphs.twinStepOptionMapFlatMapJob[Storm, Int, Int, Int, Int](_,_)(fnA, fnB)
      )


    Equiv[Map[Int, Int]].equiv(
      TestGraphs.twinStepOptionMapFlatMapScala(original)(fnA, fnB),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

  "OptionMap to nothing and FlatMap" in {
    val original = sample[List[Int]]
    val fnA = {(x: Int) => None }
    val fnB = sample[Int => List[(Int,Int)]]

    val returnedState =
      runOnce(original)(
        TestGraphs.twinStepOptionMapFlatMapJob[Storm, Int, Int, Int, Int](_,_)(fnA, fnB)
      )
    Equiv[Map[Int, Int]].equiv(
      TestGraphs.twinStepOptionMapFlatMapScala(original)(fnA, fnB),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

  "StormPlatform matches Scala for large expansion single step jobs" in {
    val original = sample[List[Int]]
    val expander = sample[Int => List[(Int, Int)]]
    val expansionFunc = {(x: Int) =>
                          expander(x).flatMap{case (k, v) => List((k, v), (k, v), (k, v), (k, v), (k, v))}
                        }
    val returnedState =
      runOnce(original)(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_,_)(expansionFunc)
      )

    Equiv[Map[Int, Int]].equiv(
      TestGraphs.singleStepInScala(original)(expansionFunc),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

   "StormPlatform matches Scala for flatmap keys jobs" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7,  8, 9, 10, 11, 12, 13 ,41, 1, 2, 3, 4, 5, 6, 7,  8, 9, 10, 11, 12, 13 ,41) // sample[List[Int]]
    val fnA = sample[Int => List[(Int, Int)]]
    val fnB = sample[Int => List[Int]]
    val returnedState =
      runOnce(original)(
        TestGraphs.singleStepMapKeysJob[Storm, Int, Int, Int, Int](_,_)(fnA, fnB)
      )

    Equiv[Map[Int, Int]].equiv(
      TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

  "StormPlatform matches Scala for left join jobs" in {
    val original = sample[List[Int]]
    val staticFunc = { i: Int => List((i -> i)) }
    val returnedState =
      runOnce(original)(
        TestGraphs.leftJoinJob[Storm, Int, Int, Int, Int, Int](_, service, _)(staticFunc)(nextFn)
      )

    Equiv[Map[Int, Int]].equiv(
      TestGraphs.leftJoinInScala(original)(serviceFn)
        (staticFunc)(nextFn),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

  "StormPlatform matches Scala for optionMap only jobs" in {
    val original = sample[List[Int]]
    val (id, store) = genStore

    val cluster = new LocalCluster()

    val producer =
      Storm.source(TraversableSpout(original))
        .filter(_ % 2 == 0)
        .map(_ -> 10)
        .sumByKey(store)

    val topo = storm.plan(producer)
    StormRunner.run(topo)

    Equiv[Map[Int, Int]].equiv(
        MapAlgebra.sumByKey(original.filter(_ % 2 == 0).map(_ -> 10)),
        globalState(id).store.asScala
          .toMap
          .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

  "StormPlatform matches Scala for MapOnly/NoSummer" in {
    val original = sample[List[Int]]
    val doubler = {x: Int => List(x*2)}

    val stormOutputList =
      runWithOutSummer(original)(
        TestGraphs.mapOnlyJob[Storm, Int, Int](_, _)(doubler)
      ).sorted


    val memoryOutputList =
      memoryPlanWithoutSummer(original) (TestGraphs.mapOnlyJob[Memory, Int, Int](_, _)(doubler)).sorted

    stormOutputList must_==(memoryOutputList)
  }

  "StormPlatform with multiple summers" in {
    val original = List(1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7,  8, 9, 10, 11, 12, 13 ,41, 1, 2, 3, 4, 5, 6, 7,  8, 9, 10, 11, 12, 13 ,41) // sample[List[Int]]
    val doubler = {(x): (Int) => List((x -> x*2))}
    val simpleOp = {(x): (Int) => List(x * 10)}

    val source = Storm.source(TraversableSpout(original))
    val (store1Id, store1) = genStore
    val (store2Id, store2) = genStore

    val tail = TestGraphs.multipleSummerJob[Storm, Int, Int, Int, Int, Int, Int](source, store1, store2)(simpleOp, doubler, doubler)

    val topo = storm.plan(tail)
    StormRunner.run(topo)

    val (scalaA, scalaB) = TestGraphs.multipleSummerJobInScala(original)(simpleOp, doubler, doubler)

    val store1Map = globalState(store1Id).store.asScala.toMap
    val store2Map = globalState(store2Id).store.asScala.toMap
    Equiv[Map[Int, Int]].equiv(
      scalaA,
      store1Map
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue

    Equiv[Map[Int, Int]].equiv(
      scalaB,
      store2Map
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
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
    val service = StoreWrapper[Int, Int](() => ReadableStore.fromFn(serviceFn))

    val tail = TestGraphs.realJoinTestJob[Storm, Int, Int, Int, Int, Int, Int, Int, Int, Int](source1, source2, source3, source4,
                service, store1, fn1, fn2, fn3, preJoinFn, postJoinFn)

    val topo = storm.plan(tail)
    OnlinePlan(tail).nodes.size must beLessThan(10)

    StormRunner.run(topo)


    val scalaA = TestGraphs.realJoinTestJobInScala(original1, original2, original3, original4,
                serviceFn, fn1, fn2, fn3, preJoinFn, postJoinFn)

    val store1Map = globalState(store1Id).store.asScala.toMap
    Equiv[Map[Int, Int]].equiv(
      scalaA,
      store1Map
        .collect { case ((k, batchID), Some(v)) => (k, v) }
    ) must beTrue
  }

}
