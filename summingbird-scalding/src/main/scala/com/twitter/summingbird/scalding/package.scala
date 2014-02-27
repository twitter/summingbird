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

package com.twitter.summingbird

import com.twitter.scalding.TypedPipe

import com.twitter.scalding.Mode
import cascading.flow.FlowDef

import com.twitter.algebird.monad.{Reader, StateWithError}
import com.twitter.algebird.Interval
import com.twitter.summingbird
import com.twitter.summingbird.batch.Timestamp
import org.apache.hadoop.io.Writable

package object scalding {
  /** How we represent the streams in scalding */
  type TimedPipe[+T] = TypedPipe[(Timestamp, T)]
  type KeyValuePipe[+K, +V] = TimedPipe[(K, V)]
  /** The Platform recursively passes this input around to describe a
   * step forward: requested input time span, and scalding Mode
   */
  type FactoryInput = (Interval[Timestamp], Mode)
  /** When it is time to run build the final flow,
   * this is what scalding needs. It is modified in the Reader[FlowInput, T]
   */
  type FlowInput = (FlowDef, Mode)
  /** This is a function that modifies a flow to return T
   * generally T will be some kind of TypedPipe
   */
  type FlowProducer[+T] = Reader[FlowInput, T]
  /** We so commonly talk about producing TimedPipe we define this
   */
  type FlowToPipe[+T] = FlowProducer[TimedPipe[T]]
  /** These are printed/logged only when we can't make any progress */
  type FailureReason = String

  type Try[+T] = Either[List[FailureReason], T]

  /** The recursive planner produces these objects which are Monads */
  type PlannerOutput[+T] = StateWithError[FactoryInput, List[FailureReason], T]
  /** We are usually producing Pipes in the Planner */
  type PipeFactory[+T] = PlannerOutput[FlowToPipe[T]]

  // Helps interop with scalding:
  implicit def modeFromTuple(implicit fm: (FlowDef, Mode)): Mode = fm._2
  implicit def flowDefFromTuple(implicit fm: (FlowDef, Mode)): FlowDef = fm._1
  implicit def toPipeFactoryOps[T](pipeF: PipeFactory[T]) = new PipeFactoryOps(pipeF)

  def toTry(e: Throwable): Try[Nothing] = {
    val writer = new java.io.StringWriter
    val printWriter = new java.io.PrintWriter(writer)
    e.printStackTrace(printWriter)
    printWriter.flush
    Left(List(writer.toString))
  }

  val ScaldingConfig = summingbird.batch.BatchConfig


  // ALL DEPRECATION ALIASES BELOW HERE, NOTHING ELSE.
  @deprecated("Use com.twitter.summingbird.batch.WaitingState", "0.3.2")
  type WaitingState[T] = summingbird.batch.WaitingState[T]
  @deprecated("Use com.twitter.summingbird.batch.PrepareState", "0.3.2")
  type PrepareState[T] = summingbird.batch.PrepareState[T]
  @deprecated("Use com.twitter.summingbird.batch.RunningState", "0.3.2")
  type RunningState[T] = summingbird.batch.RunningState[T]

  @deprecated("Use com.twitter.summingbird.scalding.service.SimpleService", "0.3.2")
  type SimpleService[K, V] = com.twitter.summingbird.scalding.service.SimpleService[K, V]

  @deprecated("Use com.twitter.summingbird.scalding.batch.BatchedService", "0.3.2")
  type BatchedService[K, V] = com.twitter.summingbird.scalding.batch.BatchedService[K, V]

  @deprecated("Use com.twitter.summingbird.scalding.batch.BatchedService", "0.3.2")
  val BatchedService = com.twitter.summingbird.scalding.batch.BatchedService

  @deprecated("com.twitter.summingbird.scalding.store.VersionedBatchStore", "0.3.2")
  type VersionedBatchStore[K, V, W, X] = com.twitter.summingbird.scalding.store.VersionedBatchStore[K, V, W, X]
  val VersionedBatchStore = com.twitter.summingbird.scalding.store.VersionedBatchStore

  @deprecated("com.twitter.summingbird.scalding.store.VersionedBatchStoreBase", "0.3.2")
  type VersionedBatchStoreBase[K, V] = com.twitter.summingbird.scalding.store.VersionedBatchStoreBase[K, V]

  @deprecated("com.twitter.summingbird.scalding.batch.BatchedStore", "0.3.2")
  type BatchedScaldingStore[K, V] = com.twitter.summingbird.scalding.batch.BatchedStore[K, V]

  @deprecated("com.twitter.summingbird.scalding.Service", "0.3.2")
  type ScaldingScaldingService[K, V] = com.twitter.summingbird.scalding.Service[K, V]

  @deprecated("com.twitter.summingbird.scalding.Store", "0.3.2")
  type ScaldingStore[K, V] = com.twitter.summingbird.scalding.Store[K, V]

  @deprecated("com.twitter.summingbird.scalding.Sink", "0.3.2")
  type ScaldingSink[T] = com.twitter.summingbird.scalding.Sink[T]

  @deprecated("com.twitter.summingbird.scalding.batch.BatchedSink", "0.3.2")
  type BatchedScaldingSink[T] = com.twitter.summingbird.scalding.batch.BatchedSink[T]

  @deprecated("com.twitter.summingbird.scalding.store.InitialBatchedStore", "0.3.2")
  type InitialBatchedStore[K, V] = com.twitter.summingbird.scalding.store.InitialBatchedStore[K, V]

  @deprecated("com.twitter.summingbird.scalding.service.BatchedDeltaService", "0.3.2")
  type BatchedDeltaService[K, V] = com.twitter.summingbird.scalding.service.BatchedDeltaService[K, V]

  @deprecated("com.twitter.summingbird.scalding.service.BatchedWindowService", "0.3.2")
  type BatchedWindowService[K, V] = com.twitter.summingbird.scalding.service.BatchedWindowService[K, V]

  @deprecated("com.twitter.summingbird.scalding.service.EmptyService", "0.3.2")
  type EmptyService[K, V] = com.twitter.summingbird.scalding.service.EmptyService[K, V]

  @deprecated("com.twitter.summingbird.scalding.service.SimpleWindowedService", "0.3.2")
  type SimpleWindowedService[K, V] = com.twitter.summingbird.scalding.service.SimpleWindowedService[K, V]

  @deprecated("com.twitter.summingbird.scalding.service.UniqueKeyedService", "0.3.2")
  type UniqueKeyedService[K, V] = com.twitter.summingbird.scalding.service.UniqueKeyedService[K, V]
  val UniqueKeyedService = com.twitter.summingbird.scalding.service.UniqueKeyedService

  @deprecated("com.twitter.summingbird.scalding.source.TimePathedSource", "0.3.2")
  val TimePathedSource = com.twitter.summingbird.scalding.source.TimePathedSource

  @deprecated("com.twitter.summingbird.scalding.store.DirectoryBatchedStore", "0.3.2")
  type DirectoryBatchedStore[K <: Writable, V <: Writable] = com.twitter.summingbird.scalding.store.DirectoryBatchedStore[K, V]

  @deprecated("com.twitter.summingbird.scalding.store.VersionedStore", "0.3.2")
  val VersionedStore = com.twitter.summingbird.scalding.store.VersionedStore
}

