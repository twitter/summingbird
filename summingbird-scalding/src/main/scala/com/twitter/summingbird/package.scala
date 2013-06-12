package com.twitter.summingbird

import com.twitter.scalding.TypedPipe

import com.twitter.scalding.Mode
import cascading.flow.FlowDef

import com.twitter.summingbird.monad.{Reader, StateWithError}
import com.twitter.summingbird.batch.Interval

package object scalding {
  /** We represent time as Long Millis */
  type Time = Long
  /** How we represent the streams in scalding */
  type TimedPipe[+T] = TypedPipe[(Time, T)]
  type KeyValuePipe[+K, +V] = TimedPipe[(K, V)]
  /** The Platform recursively passes this input around to describe a
   * step forward: requested input time span, and scalding Mode
   */
  type FactoryInput = (Interval[Time], Mode)
  /** When it is time to run build the final flow,
   * this is what scalding needs. It is modified in the Reader[FlowInput, T]
   */
  type FlowInput = (FlowDef, Mode)
  /** This is a function that modifies a flow to return T
   * generally T will be som ekind of TypedPipe
   */
  type FlowProducer[+T] = Reader[FlowInput, T]
  /** We so commonly talk about producing TimedPipe we define this
   */
  type FlowToPipe[+T] = FlowProducer[TimedPipe[T]]
  /** These are printed/logged only when we can't make any progress */
  type FailureReason = String
  // TODO replace with scala Try with 2.9.3 or greater
  type Try[T] = Either[List[FailureReason], T]

  /** The recursive planner produces these objects which are Monads */
  type PlannerOutput[+T] = StateWithError[FactoryInput, List[FailureReason], T]
  /** We are usually producing Pipes in the Planner */
  type PipeFactory[+T] = PlannerOutput[FlowToPipe[T]]
}
