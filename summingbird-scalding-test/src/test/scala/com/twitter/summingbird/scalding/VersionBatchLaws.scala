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

import com.twitter.summingbird.batch._

import org.scalacheck.Prop._
import org.scalacheck.Properties

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
