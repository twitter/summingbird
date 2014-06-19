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

package com.twitter.summingbird.sink

import cascading.flow.FlowDef

import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.scalding.{ Mode, TypedPipe }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.scalding.batch.BatchedSink

/**
 * Represents a location to which intermediate results of the
 * "flatMap" operation can be written for consumption by other
 * jobs. On the offline side, this can be a time-based source on HDFS
 * with one file per each batch ID. On the online side, this can be a
 * kestrel fanout or kafka topic.
 */

// @deprecated("ignores time", "0.1.0")
trait OfflineSink[Event] {
  def write(batchID: BatchID, pipe: TypedPipe[Event])(implicit fd: FlowDef, mode: Mode)
}

/** Wrapped for the new scalding sink API in terms of the above */
class BatchedSinkFromOffline[T](override val batcher: Batcher, offline: OfflineSink[T]) extends BatchedSink[T] {
  /**
   * OfflineSink doesn't support reading
   */
  def readStream(batchID: BatchID, mode: Mode) = None

  /**
   * Instances may choose to write out materialized streams
   * by implementing this. This is what readStream returns.
   */
  def writeStream(batchID: BatchID, stream: TypedPipe[(Timestamp, T)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    // strip the time
    offline.write(batchID, stream.values)(flowDef, mode)
  }
}

case class CompoundSink[Event](offline: Option[OfflineSink[Event]], online: Option[() => OnlineSink[Event]])

object CompoundSink {
  def apply[Event](offline: OfflineSink[Event], online: => OnlineSink[Event]): CompoundSink[Event] =
    CompoundSink(Some(offline), Some(() => online))

  def fromOffline[Event](offline: OfflineSink[Event]): CompoundSink[Event] =
    CompoundSink(Some(offline), None)

  def fromOnline[Event](online: => OnlineSink[Event]): CompoundSink[Event] =
    CompoundSink(None, Some(() => online))
}
