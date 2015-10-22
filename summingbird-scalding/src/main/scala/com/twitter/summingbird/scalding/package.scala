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

import com.twitter.algebird.monad.{ Reader, StateWithError }
import com.twitter.algebird.Interval
import com.twitter.summingbird
import com.twitter.summingbird.batch.Timestamp
import org.apache.hadoop.io.Writable

package object scalding {
  /** How we represent the streams in scalding */
  type TimedPipe[+T] = TypedPipe[(Timestamp, T)]
  type KeyValuePipe[+K, +V] = TimedPipe[(K, V)]
  /**
   * The Platform recursively passes this input around to describe a
   * step forward: requested input time span, and scalding Mode
   */
  type FactoryInput = (Interval[Timestamp], Mode)
  /**
   * When it is time to run build the final flow,
   * this is what scalding needs. It is modified in the Reader[FlowInput, T]
   */
  type FlowInput = (FlowDef, Mode)
  /**
   * This is a function that modifies a flow to return T
   * generally T will be some kind of TypedPipe
   */
  type FlowProducer[+T] = Reader[FlowInput, T]
  /**
   * We so commonly talk about producing TimedPipe we define this
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

  def toTry(e: Throwable, msg: String = ""): Try[Nothing] = {
    val writer = new java.io.StringWriter
    val printWriter = new java.io.PrintWriter(writer)
    e.printStackTrace(printWriter)
    printWriter.flush
    Left(List(msg + writer.toString))
  }
}

