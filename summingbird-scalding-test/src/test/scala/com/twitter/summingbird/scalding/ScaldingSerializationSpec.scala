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

import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import cascading.tuple.{ Tuple, Fields, TupleEntry }
import cascading.flow.FlowDef
import cascading.tap.Tap
import cascading.scheme.NullScheme
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector

import org.scalatest.WordSpec
import com.twitter.scalding.Config

class ScaldingSerializationSpecs extends WordSpec {
  implicit def tupleExtractor[T <: (Long, _)]: TimeExtractor[T] = TimeExtractor(_._1)

  "ScaldingPlatform" should {
    "serialize Hadoop Jobs for single step jobs" in {
      // Add a time:
      val inWithTime = List(1, 2, 3).zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = TestUtil.randomBatcher(inWithTime)
      val testStore = TestStore[Int, Int]("test", batcher, Iterable.empty, inWithTime.size)
      val (buffer, source) = TestSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding, (Long, Int), Int, Int](source, testStore) {
        tup => List((1 -> tup._2))
      }

      val mode = HadoopTest(new Configuration, { case x: ScaldingSource => buffer.get(x) })
      val intr = Interval.leftClosedRightOpen(Timestamp(0L), Timestamp(inWithTime.size.toLong))
      val scald = Scalding("scalaCheckJob")

      assert((try { scald.toFlow(Config.default, intr, mode, scald.plan(summer)); true }
      catch { case t: Throwable => println(toTry(t)); false }) == true)
    }
  }
}
