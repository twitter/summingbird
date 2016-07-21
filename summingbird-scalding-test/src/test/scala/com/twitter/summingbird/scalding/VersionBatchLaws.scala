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

object VersionBatchLaws extends Properties("VersionBatchLaws") {
  property("version -> BatchID -> version") = forAll { (l: Long) =>
    (l == Long.MinValue) || {
      // This law is only true for numbers greater than MinValue
      val vbs = new store.VersionedBatchStore[Int, Int, Array[Byte], Array[Byte]](null,
        0, Batcher.ofHours(1))(null)(null)
      val b = vbs.versionToBatchID(l)
      vbs.batchIDToVersion(b) <= l
    }
  }
  property("BatchID -> version -> BatchID") = forAll { (bint: Int) =>
    val b = BatchID(bint)
    val vbs = new store.VersionedBatchStore[Int, Int, Array[Byte], Array[Byte]](null,
      0, Batcher.ofHours(1))(null)(null)
    val v = vbs.batchIDToVersion(b)
    vbs.versionToBatchID(v) == b
  }
  property("version is an upperbound on time") = forAll { (lBig: Long) =>
    val l = lBig / 1000L
    val batcher = Batcher.ofHours(1)
    val vbs = new store.VersionedBatchStore[Int, Int, Array[Byte], Array[Byte]](null,
      0, batcher)(null)(null)
    val b = vbs.versionToBatchID(l)
    (batcher.earliestTimeOf(b.next).milliSinceEpoch <= l) &&
      (batcher.earliestTimeOf(b).milliSinceEpoch < l)
    (batcher.earliestTimeOf(b.next.next).milliSinceEpoch > l)
  }
}

