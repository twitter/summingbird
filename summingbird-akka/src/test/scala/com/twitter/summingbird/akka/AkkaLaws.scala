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

package com.twitter.summingbird.akka

import com.twitter.algebird.{ MapAlgebra, Monoid }
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.util.Future
import java.util.{ Collections, HashMap, Map => JMap, UUID }
import java.util.concurrent.atomic.AtomicInteger
import org.specs._
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
/**
 * Tests for Summingbird's Akka planner.
 */

/**
 * State required to perform a single Storm test run.
 */
case class TestState[T, K, V](
  store: JMap[(K, BatchID), Option[V]] = Collections.synchronizedMap(new HashMap[(K, BatchID), Option[V]]()),
  used: ArrayBuffer[T] = new ArrayBuffer[T] with SynchronizedBuffer[T],
  placed: AtomicInteger = new AtomicInteger)

object AkkaLaws extends Specification {
  import MapAlgebra.sparseEquiv

  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  def createGlobalState[T, K, V] =
    new MutableHashMap[String, TestState[T, K, V]] with SynchronizedMap[String, TestState[T, K, V]]

  /**
   * Global state shared by all tests.
   */
  val globalState = createGlobalState[Int, Int, Int]

  def runJob(p: TailProducer[Akka, _]) = {
    val cluster = Akka.local()
    cluster.run(p, "Test Job")
    Thread.sleep(300)
    cluster.shutdown
    cluster.awaitTermination
  }

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

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  /**
   * Perform a single run of TestGraphs.singleStepJob using the
   * supplied list of integers and the testFn defined above.
   */
  def runOnce(original: List[Int])(mkJob: (Producer[Akka, Int], Akka#Store[Int, Int]) => TailProducer[Akka, (Int, Int)]): (Int => TraversableOnce[(Int, Int)], TestState[Int, Int, Int]) = {
    val id = UUID.randomUUID.toString
    globalState += (id -> TestState())

    val job = mkJob(
      Akka.source(AkkaSource.fromTraversable(original)),
      MergeableStoreSupplier(() => testingStore(id), Batcher.unit))

    runJob(job)
    (testFn, globalState(id))
  }

  "AkkaPlatform matches Scala for single step jobs" in {
    val original = sample[List[Int]]
    val (fn, returnedState) =
      runOnce(original)(
        TestGraphs.singleStepJob[Akka, Int, Int, Int](_, _)(testFn))
    Equiv[Map[Int, Int]].equiv(
      TestGraphs.singleStepInScala(original)(fn),
      returnedState.store.asScala.toMap
        .collect { case ((k, batchID), Some(v)) => (k, v) }) must beTrue
  }

  val nextFn = { pair: ((Int, (Int, Option[Int]))) =>
    val (k, (v, joinedV)) = pair
    List((k -> joinedV.getOrElse(10)))
  }

  val serviceFn = Arbitrary.arbitrary[Int => Option[Int]].sample.get
  val service = StoreWrapper[Int, Int](() => ReadableStore.fromFn(serviceFn))

    "AkkaPlatform matches Scala for left join jobs" in {
      val original = sample[List[Int]]
  
      val (fn, returnedState) =
        runOnce(original)(
          TestGraphs.leftJoinJob[Akka, Int, Int, Int, Int, Int](_, service, _)(testFn)(nextFn)
        )
      Equiv[Map[Int, Int]].equiv(
        TestGraphs.leftJoinInScala(original)(serviceFn)
          (fn)(nextFn),
        returnedState.store.asScala.toMap
          .collect { case ((k, batchID), Some(v)) => (k, v) }
      ) must beTrue
    }

    "AkkaPlatform matches Scala for optionMap only jobs" in {
      val original = List(3, 4, 5) //sample[List[Int]]
      val id = UUID.randomUUID.toString
  
      globalState += (id -> TestState())
  
      val producer =
        Akka.source(AkkaSource.fromTraversable(original))
          .filter(_ % 2 == 0)
          .map(_ -> 10)
          .sumByKey(Akka.store(testingStore(id)))
  
      runJob(producer)
      Equiv[Map[Int, Int]].equiv(
          MapAlgebra.sumByKey(original.filter(_ % 2 == 0).map(_ -> 10)),
          globalState(id).store.asScala
            .toMap
            .collect { case ((k, batchID), Some(v)) => (k, v) }
      ) must beTrue
    }
}
