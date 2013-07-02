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

import com.twitter.summingbird.batch.{ BatchID, Batcher, Interval }
import com.twitter.scalding.{ Mode, TypedPipe }
import com.twitter.summingbird.scalding.{ScaldingEnv, BatchedScaldingSink}

/**
 * Represents a location to which intermediate results of the
 * "flatMap" operation can be written for consumption by other
 * jobs. On the offline side, this can be a time-based source on HDFS
 * with one file per each batch ID. On the online side, this can be a
 * kestrel fanout or kafka topic.
 */

//@deprecated("now","the ScaldingEnv was not suficiently extendable, ignores time")
trait OfflineSink[Event] {
  def write(pipe: TypedPipe[Event])
  (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv)
}

/** Wrapped for the new scalding sink API in terms of the above */
class BatchedSinkFromOffline[T](override val batcher: Batcher, offline: OfflineSink[T]) extends BatchedScaldingSink[T] {
  /** OfflineSink doesn't support reading
   */
  def readStream(batchID: BatchID, mode: Mode) = None

  /** Instances may choose to write out materialized streams
   * by implementing this. This is what readStream returns.
   */
  def writeStream(batchID: BatchID, stream: TypedPipe[(Long, T)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    val fakeEnv = new ScaldingEnv("fakeArgs", Array()) {
      override def startBatch(b: Batcher) = Some(batchID)
      override def batches = 1
    }
    // strip the time
    offline.write(stream.values)(flowDef, mode, fakeEnv)
  }
}

class EmptyOfflineSink[Event] extends OfflineSink[Event] {
  def write(pipe: TypedPipe[Event])
    (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv) {}
}

case class CompoundSink[Event](offline: OfflineSink[Event], online: () => OnlineSink[Event])

object CompoundSink {
  def fromOffline[Event](offline: OfflineSink[Event]): CompoundSink[Event] =
    CompoundSink(offline, () => new EmptyOnlineSink[Event]())

  def fromOnline[Event](online: => OnlineSink[Event]): CompoundSink[Event] =
    CompoundSink(new EmptyOfflineSink[Event](), () => online)
}
