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

package com.twitter.summingbird.scalding

import com.twitter.algebird.{ MapAlgebra, Monoid, Group, Interval, Last }
import com.twitter.algebird.monad._
import com.twitter.summingbird.{ Producer, TimeExtractor, TestGraphs }
import com.twitter.summingbird.batch._
import com.twitter.summingbird.batch.state.HDFSState
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.SummingbirdRuntimeStats

import java.util.TimeZone
import java.io.File

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }
import com.twitter.scalding.typed.TypedSink

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import scala.collection.mutable.{ ArrayBuffer, Buffer, HashMap => MutableHashMap, Map => MutableMap, SynchronizedBuffer, SynchronizedMap }
import scala.util.{ Try => ScalaTry }

import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import cascading.tuple.{ Tuple, Fields, TupleEntry }
import cascading.flow.Flow
import cascading.stats.FlowStats
import cascading.tap.Tap
import cascading.scheme.NullScheme
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector

import org.scalatest.WordSpec

/**
 * Tests for Summingbird's Scalding planner.
 */

class ScaldingLaws extends WordSpec {
  import MapAlgebra.sparseEquiv

  implicit def timeExtractor[T <: (Long, _)] = TestUtil.simpleTimeExtractor[T]

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  "The ScaldingPlatform" should {

    //Set up the job:
    "match scala for single step jobs" in {
      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher)

      val inMemory = TestGraphs.singleStepInScala(batchCoveredInput)(fn)

      val initStore = sample[Map[Int, Int]]
      val testStore = TestStore[Int, Int]("test", batcher, initStore, inWithTime.size)
      val (buffer, source) = TestSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding, (Long, Int), Int, Int](source, testStore)(t =>
        fn(t._2))

      val scald = Scalding("scalaCheckJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(t => (testStore.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(summer))
      // Now check that the inMemory ==

      assert(TestUtil.compareMaps(original, Monoid.plus(initStore, inMemory), testStore) == true)
    }

    "match scala single step pruned jobs" in {
      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]
      val prunedList = sample[Set[Int]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher)

      val inMemory = {
        val computedMap = TestGraphs.singleStepInScala(batchCoveredInput)(fn)
        val totalMap = Monoid.plus(initStore, computedMap)
        totalMap.filter(kv => !prunedList.contains(kv._1)).toMap
      }

      val pruner = new PrunedSpace[(Int, Int)] {
        def prune(item: (Int, Int), writeTime: Timestamp) = {
          prunedList.contains(item._1)
        }
      }

      val testStore = TestStore[Int, Int]("test", batcher, initStore, inWithTime.size, pruner)
      val (buffer, source) = TestSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding, (Long, Int), Int, Int](source, testStore)(t =>
        fn(t._2))

      val scald = Scalding("scalaCheckJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(t => (testStore.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(summer))
      // Now check that the inMemory ==

      assert(TestUtil.compareMaps(original, inMemory, testStore) == true)
    }

    "match scala for flatMapKeys jobs" in {
      val original = sample[List[Int]]
      val initStore = sample[Map[Int, Int]]
      val fnA = sample[(Int) => List[(Int, Int)]]
      val fnB = sample[Int => List[Int]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher)

      val inMemory = TestGraphs.singleStepMapKeysInScala(batchCoveredInput)(fnA, fnB)

      val testStore = TestStore[Int, Int]("test", batcher, initStore, inWithTime.size)
      val (buffer, source) = TestSource(inWithTime)

      val summer = TestGraphs.singleStepMapKeysJob[Scalding, (Long, Int), Int, Int, Int](source, testStore)(t =>
        fnA(t._2), fnB)

      val scald = Scalding("scalaCheckJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(t => (testStore.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(summer))
      // Now check that the inMemory ==

      assert(TestUtil.compareMaps(original, Monoid.plus(initStore, inMemory), testStore) == true)
    }

    "match scala for multiple summer jobs" in {
      val original = sample[List[Int]]
      val initStoreA = sample[Map[Int, Int]]
      val initStoreB = sample[Map[Int, Int]]
      val fnA = sample[(Int) => List[(Int)]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val fnC = sample[(Int) => List[(Int, Int)]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher).toList

      val (inMemoryA, inMemoryB) = TestGraphs.multipleSummerJobInScala(batchCoveredInput)(fnA, fnB, fnC)

      val testStoreA = TestStore[Int, Int]("testA", batcher, initStoreA, inWithTime.size)
      val testStoreB = TestStore[Int, Int]("testB", batcher, initStoreB, inWithTime.size)
      val (buffer, source) = TestSource(inWithTime)

      val tail = TestGraphs.multipleSummerJob[Scalding, (Long, Int), Int, Int, Int, Int, Int](source, testStoreA, testStoreB)({ t => fnA(t._2) }, fnB, fnC)

      val scald = Scalding("scalaCheckMultipleSumJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(t => (testStoreA.sourceToBuffer ++ testStoreB.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(tail))
      // Now check that the inMemory ==

      assert(TestUtil.compareMaps(original, Monoid.plus(initStoreA, inMemoryA), testStoreA) == true)
      assert(TestUtil.compareMaps(original, Monoid.plus(initStoreB, inMemoryB), testStoreB) == true)
    }

    "match scala for leftJoin jobs" in {
      val original = sample[List[Int]]
      val prejoinMap = sample[(Int) => List[(Int, Int)]]
      val service = sample[(Int, Int) => Option[Int]]
      val postJoin = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]

      //Add a time
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher).toIterable

      // We need to keep track of time correctly to use the service
      var fakeTime = -1
      val timeIncIt = new Iterator[Int] {
        val inner = batchCoveredInput.iterator
        def hasNext = inner.hasNext
        def next = {
          fakeTime += 1
          inner.next
        }
      }
      val srvWithTime = { (key: Int) => service(fakeTime, key) }

      val inMemory = TestGraphs.leftJoinInScala(timeIncIt)(srvWithTime)(prejoinMap)(postJoin)

      // Add a time:
      val allKeys = original.flatMap(prejoinMap).map { _._1 }
      val allTimes = (0 until original.size)
      val stream = for { time <- allTimes; key <- allKeys; v = service(time, key) } yield (time.toLong, (key, v))

      val initStore = sample[Map[Int, Int]]
      val testStore = TestStore[Int, Int]("test", batcher, initStore, inWithTime.size)

      /**
       * Create the batched service
       */
      val batchedService = stream.map { case (time, v) => (Timestamp(time), v) }.groupBy { case (ts, _) => batcher.batchOf(ts) }
      val testService = new TestService[Int, Int]("srv", batcher, batcher.batchOf(Timestamp(0)).prev, batchedService)

      val (buffer, source) = TestSource(inWithTime)

      val summer =
        TestGraphs.leftJoinJob[Scalding, (Long, Int), Int, Int, Int, Int](source, testService, testStore) { tup => prejoinMap(tup._2) }(postJoin)

      val scald = Scalding("scalaCheckleftJoinJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(s => (testStore.sourceToBuffer ++ buffer ++ testService.sourceToBuffer).get(s))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      assert(TestUtil.compareMaps(original, Monoid.plus(initStore, inMemory), testStore) == true)
    }

    "match scala for leftJoin  repeated tuple leftJoin jobs" in {
      val original = sample[List[Int]]
      val prejoinMap = sample[(Int) => List[(Int, Int)]]
      val service = sample[(Int, Int) => Option[Int]]
      val postJoin = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher).toIterable

      // We need to keep track of time correctly to use the service
      var fakeTime = -1
      val timeIncIt = new Iterator[Int] {
        val inner = batchCoveredInput.iterator
        def hasNext = inner.hasNext
        def next = {
          fakeTime += 1
          inner.next
        }
      }
      val srvWithTime = { (key: Int) => service(fakeTime, key) }
      val inMemory = TestGraphs.repeatedTupleLeftJoinInScala(timeIncIt)(srvWithTime)(prejoinMap)(postJoin)

      // Add a time:
      val allKeys = original.flatMap(prejoinMap).map { _._1 }
      val allTimes = (0 until original.size)
      val stream = for { time <- allTimes; key <- allKeys; v = service(time, key) } yield (time.toLong, (key, v))

      val initStore = sample[Map[Int, Int]]
      val testStore = TestStore[Int, Int]("test", batcher, initStore, inWithTime.size)

      /**
       * Create the batched service
       */
      val batchedService = stream.map { case (time, v) => (Timestamp(time), v) }.groupBy { case (ts, _) => batcher.batchOf(ts) }
      val testService = new TestService[Int, Int]("srv", batcher, batcher.batchOf(Timestamp(0)).prev, batchedService)

      val (buffer, source) = TestSource(inWithTime)

      val summer =
        TestGraphs.repeatedTupleLeftJoinJob[Scalding, (Long, Int), Int, Int, Int, Int](source, testService, testStore) { tup => prejoinMap(tup._2) }(postJoin)

      val scald = Scalding("scalaCheckleftJoinJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(s => (testStore.sourceToBuffer ++ buffer ++ testService.sourceToBuffer).get(s))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      assert(TestUtil.compareMaps(original, Monoid.plus(initStore, inMemory), testStore) == true)
    }

    "match scala for leftJoin with store (no dependency between the two) jobs" in {
      // TODO: what if the two sources are of different sizes here?
      val original1 = sample[List[Int]]
      val original2Fn = sample[(Int) => Int]
      val original2 = original1.map { v => original2Fn(v) }

      val fnA = sample[(Int) => List[(Int, Int)]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val postJoin = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]

      // Add a time
      val inWithTime1 = original1.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val inWithTime2 = original2.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original1.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime1)
      val batchCoveredInput1 = TestUtil.pruneToBatchCoveredWithTime(inWithTime1, intr, batcher)
      val batchCoveredInput2 = TestUtil.pruneToBatchCoveredWithTime(inWithTime2, intr, batcher)

      def toTime[T, U](fn: T => TraversableOnce[U]): ((Long, T)) => TraversableOnce[(Long, U)] =
        (x: (Long, T)) => fn(x._2).map((x._1, _))

      val fnAWithTime = toTime(fnA)
      val fnBWithTime = toTime(fnB)
      val postJoinWithTime = toTime(postJoin)

      val (inMemoryA, inMemoryB) =
        TestGraphs.leftJoinWithStoreInScala(batchCoveredInput1, batchCoveredInput2)(fnAWithTime)(fnBWithTime)(postJoinWithTime)

      val storeAndServiceInit = sample[Map[Int, Int]]
      val storeAndServiceStore = TestStore[Int, Int]("storeAndService", batcher, storeAndServiceInit, inWithTime1.size)
      val storeAndService = TestStoreService[Int, Int](storeAndServiceStore)

      val finalStoreInit = sample[Map[Int, Int]]
      val finalStore = TestStore[Int, Int]("finalStore", batcher, finalStoreInit, inWithTime1.size)

      // the end range needs to be multiple of batchsize
      val endTimeOfLastBatch1 = batcher.latestTimeOf(batcher.batchOf(Timestamp(inWithTime1.size))).milliSinceEpoch
      val endTimeOfLastBatch2 = batcher.latestTimeOf(batcher.batchOf(Timestamp(inWithTime2.size))).milliSinceEpoch
      val (buffer1, source1) = TestSource(inWithTime1, Some(DateRange(RichDate(0), RichDate(endTimeOfLastBatch1))))
      val (buffer2, source2) = TestSource(inWithTime2, Some(DateRange(RichDate(0), RichDate(endTimeOfLastBatch2))))

      val summer =
        TestGraphs.leftJoinWithStoreJob[Scalding, (Long, Int), (Long, Int), Int, Int, Int, Int](source1, source2,
          storeAndService,
          finalStore)(tup => fnA(tup._2))(tup => fnB(tup._2))(postJoin)

      val scald = Scalding("scalaCheckleftJoinWithStoreJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode((storeAndService.sourceToBuffer ++ finalStore.sourceToBuffer ++ buffer1 ++ buffer2).get(_))

      scald.run(ws, mode, summer)

      // Now check that the inMemory ==
      assert(TestUtil.compareMaps(original1, Monoid.plus(storeAndServiceInit, inMemoryA), storeAndServiceStore) == true)
      assert(TestUtil.compareMaps(original2, Monoid.plus(finalStoreInit, inMemoryB), finalStore) == true)
    }

    "match scala for leftJoin with store (with dependency between store and join) jobs" in {
      val original = sample[List[Int]]

      val fnA = sample[(Int) => List[(Int, Int)]]

      // compose multiple flatMapValues functions
      val valuesFlatMap1 = sample[((Int, Option[Int])) => List[String]]
      val valuesFlatMap2 = sample[(String) => List[Int]]

      val valuesFlatMap =
        (e: ((Int, Option[Int]))) =>
          valuesFlatMap1(e).flatMap { x => { valuesFlatMap2(x) } }

      def toTime[T, U](fn: T => TraversableOnce[U]): ((Long, T)) => TraversableOnce[(Long, U)] =
        (x: (Long, T)) => fn(x._2).map((x._1, _))

      val fnAWithTime = toTime(fnA)
      val valuesFlatMapWithTime = toTime(valuesFlatMap)

      // add a time
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCoveredWithTime(inWithTime, intr, batcher)

      val inMemoryStore =
        TestGraphs.leftJoinWithDependentStoreInScala(batchCoveredInput)(fnAWithTime)(valuesFlatMapWithTime)

      val storeAndServiceInit = sample[Map[Int, Int]]
      val storeAndServiceStore = TestStore[Int, Int]("storeAndService", batcher, storeAndServiceInit, inWithTime.size)
      val storeAndService = TestStoreService[Int, Int](storeAndServiceStore)

      // the end range needs to be multiple of batchsize
      val endTimeOfLastBatch = batcher.latestTimeOf(batcher.batchOf(Timestamp(inWithTime.size))).milliSinceEpoch
      val (buffer, source) = TestSource(inWithTime, Some(DateRange(RichDate(0), RichDate(endTimeOfLastBatch))))

      val summer =
        TestGraphs.leftJoinWithDependentStoreJob[Scalding, (Long, Int), String, Int, Int, Int](source,
          storeAndService)(tup => fnA(tup._2))(valuesFlatMap1)(valuesFlatMap2)

      val scald = Scalding("scalaCheckleftJoinWithDependentJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode((storeAndService.sourceToBuffer ++ buffer).get(_))

      scald.run(ws, mode, summer)

      // Now check that the inMemory ==
      assert(TestUtil.compareMaps(original, Monoid.plus(storeAndServiceInit, inMemoryStore), storeAndServiceStore) == true)
    }

    "match scala for leftJoin with store and join fanout (with dependency between store and join) jobs" in {
      val original = sample[List[Int]]

      val fnA = sample[(Int) => List[(Int, Int)]]

      // compose multiple flatMapValues functions
      val valuesFlatMap = sample[((Int, Option[Int])) => List[Int]]
      val flatMapFn = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]

      def toTime[T, U](fn: T => TraversableOnce[U]): ((Long, T)) => TraversableOnce[(Long, U)] =
        (x: (Long, T)) => fn(x._2).map((x._1, _))

      val fnAWithTime = toTime(fnA)
      val flatMapWithTime = toTime(flatMapFn)
      val valuesFlatMapWithTime = toTime(valuesFlatMap)

      // Add a time
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCoveredWithTime(inWithTime, intr, batcher)

      val (inMemoryStoreAfterJoin, inMemoryStoreAfterFlatMap) =
        TestGraphs.leftJoinWithDependentStoreJoinFanoutInScala(batchCoveredInput)(fnAWithTime)(valuesFlatMapWithTime)(flatMapWithTime)

      val storeAndServiceInit = sample[Map[Int, Int]]
      val storeAndServiceStore = TestStore[Int, Int]("storeAndService", batcher, storeAndServiceInit, inWithTime.size)
      val storeAndService = TestStoreService[Int, Int](storeAndServiceStore)

      val fmStoreInit = sample[Map[Int, Int]]
      val fmStore = TestStore[Int, Int]("store", batcher, fmStoreInit, inWithTime.size)

      // the end range needs to be multiple of batchsize
      val endTimeOfLastBatch = batcher.latestTimeOf(batcher.batchOf(Timestamp(inWithTime.size))).milliSinceEpoch
      val (buffer, source) = TestSource(inWithTime, Some(DateRange(RichDate(0), RichDate(endTimeOfLastBatch))))

      val summer =
        TestGraphs.leftJoinWithDependentStoreJoinFanoutJob[Scalding, (Long, Int), Int, Int, Int, Int](source,
          storeAndService, fmStore)(tup => fnA(tup._2))(valuesFlatMap)(flatMapFn)

      val scald = Scalding("scalaCheckleftJoinWithDependentJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode((storeAndService.sourceToBuffer ++ buffer ++ fmStore.sourceToBuffer).get(_))

      scald.run(ws, mode, summer)

      // Now check that the inMemory ==
      assert(TestUtil.compareMaps(original, Monoid.plus(storeAndServiceInit, inMemoryStoreAfterJoin), storeAndServiceStore) == true)
      assert(TestUtil.compareMaps(original, Monoid.plus(fmStoreInit, inMemoryStoreAfterFlatMap), fmStore) == true)
    }

    "match scala for diamond jobs with write" in {
      val original = sample[List[Int]]
      val fn1 = sample[(Int) => List[(Int, Int)]]
      val fn2 = sample[(Int) => List[(Int, Int)]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher)

      val inMemory = TestGraphs.diamondJobInScala(batchCoveredInput)(fn1)(fn2)

      val initStore = sample[Map[Int, Int]]
      val testStore = TestStore[Int, Int]("test", batcher, initStore, inWithTime.size)
      val testSink = new TestSink[(Long, Int)]
      val (buffer, source) = TestSource(inWithTime)

      val summer = TestGraphs
        .diamondJob[Scalding, (Long, Int), Int, Int](source,
          testSink,
          testStore)(t => fn1(t._2))(t => fn2(t._2))

      val scald = Scalding("scalding-diamond-Job")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(s => (testStore.sourceToBuffer ++ buffer).get(s))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      val sinkOut = testSink.reset
      assert(TestUtil.compareMaps(original, Monoid.plus(initStore, inMemory), testStore) == true)
      val wrongSink = sinkOut.map { _._2 }.toList != inWithTime
      assert(wrongSink == false)
      if (wrongSink) {
        println("input: " + inWithTime)
        println("SinkExtra: " + (sinkOut.map(_._2).toSet -- inWithTime.toSet))
        println("SinkMissing: " + (inWithTime.toSet -- sinkOut.map(_._2).toSet))
      }
    }

    "Correctly aggregate multiple sumByKeys" in {
      val original = sample[List[(Int, Int)]]
      val keyExpand = sample[(Int) => List[Int]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher).toList

      val (inMemoryA, inMemoryB) = TestGraphs.twoSumByKeyInScala(batchCoveredInput, keyExpand)

      val initStore = sample[Map[Int, Int]]
      val testStoreA = TestStore[Int, Int]("testA", batcher, initStore, inWithTime.size)
      val testStoreB = TestStore[Int, Int]("testB", batcher, initStore, inWithTime.size)
      val (buffer, source) = TestSource(inWithTime)

      val summer = TestGraphs
        .twoSumByKey[Scalding, Int, Int, Int](source.map(_._2), testStoreA, keyExpand, testStoreB)

      val scald = Scalding("scalding-diamond-Job")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode((testStoreA.sourceToBuffer ++ testStoreB.sourceToBuffer ++ buffer).get(_))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      assert(TestUtil.compareMaps(original, Monoid.plus(initStore, inMemoryA), testStoreA, "A") == true)
      assert(TestUtil.compareMaps(original, Monoid.plus(initStore, inMemoryB), testStoreB, "B") == true)
    }

    "compute correct statistics" in {
      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]

      // Add a time
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batcher = TestUtil.randomBatcher(inWithTime)
      val batchCoveredInput = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher)

      val inMemory = TestGraphs.singleStepInScala(batchCoveredInput)(fn)

      val initStore = sample[Map[Int, Int]]
      val testStore = TestStore[Int, Int]("test", batcher, initStore, inWithTime.size)
      val (buffer, source) = TestSource(inWithTime)

      val jobID: JobId = new JobId("scalding.job.testJobId")
      val summer = TestGraphs.jobWithStats[Scalding, (Long, Int), Int, Int](jobID, source, testStore)(t =>
        fn(t._2))
      val scald = Scalding("scalaCheckJob").withConfigUpdater { sbconf =>
        sbconf.+("scalding.job.uniqueId", jobID.get)
      }
      val ws = new LoopState(intr)
      val conf: Configuration = new Configuration()
      val mode: Mode = HadoopTest(conf, t => (testStore.sourceToBuffer ++ buffer).get(t))

      var flow: Flow[_] = null
      scald.run(ws, mode, scald.plan(summer), { f: Flow[_] => flow = f })

      val flowStats: FlowStats = flow.getFlowStats()
      val origCounter: Long = flowStats.getCounterValue("counter.test", "orig_counter")
      val fmCounter: Long = flowStats.getCounterValue("counter.test", "fm_counter")
      val fltrCounter: Long = flowStats.getCounterValue("counter.test", "fltr_counter")
      // Now check that the stats are computed correctly
      assert(origCounter == original.size)
      assert(fmCounter == original.flatMap(fn).size * 2)
      assert(fltrCounter == original.flatMap(fn).size)
    }

    "contain and be able to init a ScaldingRuntimeStatsProvider object" in {
      val s = SummingbirdRuntimeStats.SCALDING_STATS_MODULE
      assert(ScalaTry[Unit] { Class.forName(s) }.toOption.isDefined)
    }
  }
}
