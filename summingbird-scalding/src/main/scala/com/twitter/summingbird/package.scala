package com.twitter.summingbird

import com.twitter.scalding.TypedPipe

import com.twitter.scalding.Mode
import cascading.flow.FlowDef

package object scalding {
  type Time = Long
  type Interval[-T] = batch.Interval[T]
  type TimedPipe[+T] = TypedPipe[(Time, T)]
  type KeyValuePipe[+K, +V] = TimedPipe[(K, V)]
  // After scheduling, this is the input to build the job
  type ProducerInput = (FlowDef, Mode)
  // To actually build the function to schedule, this is what we need
  type FactoryInput = (Interval[Time], ProducerInput)
  type FlowProducer[+T] = Reader[ProducerInput, T]
  type FlowToPipe[+T] = FlowProducer[TimedPipe[T]]
  type FailureReason = String
  // TODO replace with scala Try with 2.9.3 or greater
  type Try[T] = Either[List[FailureReason], T]
  type PlannerOutput[+T] = StateWithFailure[FactoryInput, List[FailureReason], T]
  // These are the planner objects which combine well
  type PipeFactory[+T] = PlannerOutput[FlowToPipe[T]]
}
