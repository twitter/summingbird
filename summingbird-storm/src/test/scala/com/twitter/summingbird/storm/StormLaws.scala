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

import backtype.storm.LocalCluster
import com.twitter.algebird.{MapAlgebra, Monoid}
import com.twitter.storehaus.JMapStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.tormenta.spout.TraversableSpout
import com.twitter.util.Future
import java.util.{Collections, HashMap, Map => JMap, UUID}
import java.util.concurrent.atomic.AtomicInteger
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap => MutableHashMap, Map => MutableMap, SynchronizedBuffer, SynchronizedMap}

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

object StormLaws extends Properties("Storm") {
  // TODO: These functions were lifted from Storehaus's testing
  // suite. They should move into Algebird to make it easier to test
  // maps that have had their zeros removed with MapAlgebra.

  def rightContainsLeft[K,V: Equiv](l: Map[K, V], r: Map[K, V]): Boolean =
    l.foldLeft(true) { (acc, pair) =>
      acc && r.get(pair._1).map { Equiv[V].equiv(_, pair._2) }.getOrElse(true)
    }

  implicit def mapEquiv[K,V: Monoid: Equiv]: Equiv[Map[K, V]] = {
    Equiv.fromFunction { (m1, m2) =>
      val cleanM1 = MapAlgebra.removeZeros(m1)
      val cleanM2 = MapAlgebra.removeZeros(m2)
      rightContainsLeft(cleanM1, cleanM2) && rightContainsLeft(cleanM2, cleanM1)
    }
  }

  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)

  def createGlobalState[T, K, V] =
    new MutableHashMap[String, TestState[T, K, V]] with SynchronizedMap[String, TestState[T, K, V]]

  /**
    * Returns a serializable iterator that wraps the supplied
    * TraversableOnce[T]. Every time "next" is accessed, the supplied
    * side-effect onNext is called with the item to be returned.
    */
  def toIterator[T](supplier: => TraversableOnce[T])(onNext: T => Unit): Iterator[T] =
    new Iterator[T] with java.io.Serializable {
      lazy val iterator = supplier.toIterator
      override def hasNext = iterator.hasNext
      override def next = {
        val ret = iterator.next
        onNext(ret)
        ret
      }
    }

  import com.twitter.summingbird.Constants.DEFAULT_SPOUT_PARALLELISM

  /**
    * Global state shared by all tests.
    */
  val globalState = createGlobalState[Int, Int, Int]

  /**
    * Returns a MergeableStore that routes get, put and merge calls
    * through to the backing store in the proper globalState entry.
    */
  def testingStore(id: String)(onMerge: () => Unit) =
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
        onMerge()
        Future.Unit
      }
      override def merge(pair: ((Int, BatchID), Int)) = {
        val (k, v) = pair
        val newV = Monoid.plus(Some(v), getOpt(k)).flatMap(Monoid.nonZeroOption(_))
        wrappedStore.put(k, newV)
        onMerge()
        Future.Unit
      }
    }

  /**
    * The function tested below. We can't generate a function with
    * ScalaCheck, as we need to know the number of tuples that the
    * flatMap will produce.
    */
  val testFn = { i: Int => List((i -> i)) }

  val storm = Storm("scalaCheckJob")

  /**
    * Perform a single run of TestGraphs.singleStepJob using the
    * supplied list of integers and the testFn defined above.
    */
  def runOnce(original: List[Int]): (Int => TraversableOnce[(Int, Int)], TestState[Int, Int, Int]) = {
    val id = UUID.randomUUID.toString
    globalState += (id -> TestState())

    val cluster = new LocalCluster()
    val items = toIterator(original)(globalState(id).used += _)

    val job = TestGraphs.singleStepJob[Storm, Int, Int, Int](
      Storm.source(new TraversableSpout(items)),
      MergeableStoreSupplier(() => testingStore(id)(() => globalState(id).placed.incrementAndGet), Batcher.unit)
    )(testFn)

    val topo = storm.buildTopology(job)
    val parallelism = DEFAULT_SPOUT_PARALLELISM.parHint

    // Submit the topology locally.
    cluster.submitTopology("testJob", storm.baseConfig, topo)

    // Wait until the topology processes all elements.
    while (globalState(id).placed.get < (original.size * parallelism)) {
      Thread.sleep(10)
    }
    cluster.shutdown
    (testFn, globalState(id))
  }

  property("StormPlatform matches Scala for single step jobs") =
    forAll { original: List[Int] =>
      val (fn, returnedState) = runOnce(original)
      Equiv[Map[Int, Int]].equiv(
        TestGraphs.singleStepInScala(returnedState.used.toList)(fn),
        returnedState.store.asScala.toMap.collect { case ((k, batchID), Some(v)) => (k, v) }
      )
    }
}
