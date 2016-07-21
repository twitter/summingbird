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
import com.twitter.summingbird.scalding.store.{ VersionedBatchStore, InitialBatchedStore }
import com.twitter.bijection._
import com.twitter.scalding.commons.source.VersionedKeyValSource

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

class VersionedBatchedStoreTest extends WordSpec {
  import MapAlgebra.sparseEquiv

  implicit def timeExtractor[T <: (Long, _)] = TestUtil.simpleTimeExtractor[T]

  private def createTempDirectory(): java.io.File = {
    val temp = File.createTempFile("temp", System.nanoTime().toString);

    if (!(temp.delete())) {
      throw new java.io.IOException("Could not delete temp file: " + temp.getAbsolutePath());
    }

    if (!(temp.mkdir())) {
      throw new java.io.IOException("Could not create temp directory: " + temp.getAbsolutePath());
    }

    temp
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  "The VersionedBatchStore" should {

    "support a multiple summer job with one store already satisfied" in {
      val rangeMax = 1000
      val original: List[Int] = (0 until rangeMax).toList // Input Data
      implicit val batcher = new MillisecondBatcher(20L)

      val lastExpectedWriteBatch = 1000

      val fnA = sample[(Int) => List[(Int)]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val fnC = sample[(Int) => List[(Int, Int)]]

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }

      // get time interval for the input
      val intr = TestUtil.toTimeInterval(0L, original.size.toLong)

      val batchCoveredInput: List[Int] = TestUtil.pruneToBatchCovered(inWithTime, intr, batcher).toList

      val (inMemoryA, inMemoryB) = TestGraphs.multipleSummerJobInScala(batchCoveredInput)(fnA, fnB, fnC)

      val packFn = { (bid: BatchID, kv: (Int, Int)) => ((bid.id, kv._1), kv._2) }
      val unpackFn = { kv: ((Long, Int), Int) => (kv._1._2, kv._2) }
      implicit val tupEncoder: Injection[(Long, Int), Array[Byte]] = Bufferable.injectionOf[(Long, Int)]
      val conf = new org.apache.hadoop.conf.Configuration
      implicit val config = Config.hadoopWithDefaults(conf)
      implicit val mode: Mode = Hdfs(true, conf)

      def buildStore(): (String, batch.BatchedStore[Int, Int], Long => Map[Int, Int]) = {
        val rootFolder = createTempDirectory().getAbsolutePath
        val testStoreVBS = VersionedBatchStore[Int, Int, (Long, Int), Int](rootFolder, 1)(packFn)(unpackFn)
        val testStore = new InitialBatchedStore(batcher.batchOf(Timestamp(inWithTime.head._1)), testStoreVBS)
        val testStoreReader = { version: Long =>
          VersionedKeyValSource[(Long, Int), Int](rootFolder, sourceVersion = Some(version)).toIterator.map(unpackFn).toMap
        }
        (rootFolder, testStore, testStoreReader)
      }

      val (_, testStoreA, testStoreAReader) = buildStore()
      val (_, testStoreB, testStoreBReader) = buildStore()
      val (_, testStoreC, testStoreCReader) = buildStore()

      val (buffer, source) = TestSource(inWithTime)

      // We are going to simulate writing to store A failed.
      // Thus we will run this twice, once with A&B and a second time with A & C
      // Stores A & C should be equal after and the jobs should succeed

      // First store's A & B
      {
        val tail = TestGraphs.multipleSummerJob[Scalding, (Long, Int), Int, Int, Int, Int, Int](source, testStoreA, testStoreB)({ t => fnA(t._2) }, fnB, fnC)
        val scald = Scalding("scalaCheckMultipleSumJob")
        val ws = new LoopState(intr)
        scald.run(ws, mode, scald.plan(tail))
        assert(!ws.failed, "Job failed")
      }

      // Now Stores C & B
      {
        val tail = TestGraphs.multipleSummerJob[Scalding, (Long, Int), Int, Int, Int, Int, Int](source, testStoreC, testStoreB)({ t => fnA(t._2) }, fnB, fnC)
        val scald = Scalding("scalaCheckMultipleSumJob")
        val ws = new LoopState(intr)
        scald.run(ws, mode, scald.plan(tail))
        assert(!ws.failed, "Job failed")
      }

      // Run stores C & B again, now there should be no operations to do
      // This should warning but succeed
      {
        val tail = TestGraphs.multipleSummerJob[Scalding, (Long, Int), Int, Int, Int, Int, Int](source, testStoreC, testStoreB)({ t => fnA(t._2) }, fnB, fnC)
        val scald = Scalding("scalaCheckMultipleSumJob")
        val ws = new LoopState(intr)
        scald.run(ws, mode, scald.plan(tail))
        assert(!ws.failed, "Job failed")
      }

      // Now check that the inMemory == matches the hadoop job we ran

      assert(TestUtil.compareMaps(original, inMemoryA, testStoreAReader(lastExpectedWriteBatch), "StoreA") == true)
      assert(TestUtil.compareMaps(original, inMemoryB, testStoreBReader(lastExpectedWriteBatch), "StoreB") == true)
      assert(TestUtil.compareMaps(original, inMemoryA, testStoreCReader(lastExpectedWriteBatch), "StoreC") == true)

      // Now for total sanity just compare store's A and C's output. it should be identical
      assert(TestUtil.compareMaps(original, testStoreAReader(lastExpectedWriteBatch),
        testStoreCReader(lastExpectedWriteBatch), "StoreC vs StoreA") == true)

    }

  }
}
