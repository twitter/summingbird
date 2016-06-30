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

class NamedOptionsSpec extends WordSpec {
  import MapAlgebra.sparseEquiv

  implicit def timeExtractor[T <: (Long, _)] = TestUtil.simpleTimeExtractor[T]

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  "The ScaldingPlatform" should {
    "named option should apply for the correct node even if same option defined for previous node" in {
      val batchCoveredInput = TestUtil.pruneToBatchCoveredWithTime(inWithTime1, intr, batcher)
      val fnAWithTime = toTime(fnA)
      val storeAndService = TestStoreService[Int, Int](storeAndServiceStore)
      val summer: Summer[P, K, JoinedU] = batchCoveredInput
        .flatMap(fnAWithTime).name("fmNode")
        .sumByKey(storeAndService).name("smNode")
    }

  }
}
