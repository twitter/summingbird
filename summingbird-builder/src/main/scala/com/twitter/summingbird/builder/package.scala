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

/**
 * We put typedefs and vals here to make working with the (deprecated) Builder
 * API nicer.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */
package object builder {

  @deprecated("Use com.twitter.summingbird.option.MonoidIsCommutative", "0.1.0")
  val MonoidIsCommutative = option.MonoidIsCommutative

  @deprecated("Use com.twitter.summingbird.option.MonoidIsCommutative", "0.1.0")
  type MonoidIsCommutative = option.MonoidIsCommutative

  /**
    * Storm flatMap option types and objects.
    */
  @deprecated("Use com.twitter.summingbird.storm.option.SpoutParallelism", "0.1.0")
  type SpoutParallelism = storm.option.SpoutParallelism

  @deprecated("Use com.twitter.summingbird.storm.option.SpoutParallelism", "0.1.0")
  val SpoutParallelism = storm.option.SpoutParallelism

  @deprecated("Use com.twitter.summingbird.storm.option.FlatMapParallelism", "0.1.0")
  type FlatMapParallelism = storm.option.FlatMapParallelism

  @deprecated("Use com.twitter.summingbird.storm.option.FlatMapParallelism", "0.1.0")
  val FlatMapParallelism = storm.option.FlatMapParallelism

  @deprecated("Use com.twitter.summingbird.storm.option.FlatMapStormMetrics", "0.1.0")
  type FlatMapStormMetrics = storm.option.FlatMapStormMetrics

  @deprecated("Use com.twitter.summingbird.storm.option.FlatMapStormMetrics", "0.1.0")
  val FlatMapStormMetrics = storm.option.FlatMapStormMetrics

  /**
    * Sink objects and types.
    */
  @deprecated("Use com.twitter.summingbird.storm.option.SinkParallelism", "0.1.0")
  type SinkParallelism = storm.option.SinkParallelism

  @deprecated("Use com.twitter.summingbird.storm.option.SinkParallelism", "0.1.0")
  val SinkParallelism = storm.option.SinkParallelism

  @deprecated("Use com.twitter.summingbird.storm.option.OnlineSuccessHandler", "0.1.0")
  type OnlineSuccessHandler = storm.option.OnlineSuccessHandler

  @deprecated("Use com.twitter.summingbird.storm.option.OnlineSuccessHandler", "0.1.0")
  val OnlineSuccessHandler = storm.option.OnlineSuccessHandler

  @deprecated("Use com.twitter.summingbird.storm.option.IncludeSuccessHandler", "0.1.0")
  type IncludeSuccessHandler = storm.option.IncludeSuccessHandler

  @deprecated("Use com.twitter.summingbird.storm.option.IncludeSuccessHandler", "0.1.0")
  val IncludeSuccessHandler = storm.option.IncludeSuccessHandler

  @deprecated("Use com.twitter.summingbird.storm.option.OnlineExceptionHandler", "0.1.0")
  type OnlineExceptionHandler = storm.option.OnlineExceptionHandler

  @deprecated("Use com.twitter.summingbird.storm.option.OnlineExceptionHandler", "0.1.0")
  val OnlineExceptionHandler = storm.option.OnlineExceptionHandler

  @deprecated("Use com.twitter.summingbird.storm.option.SummerStormMetrics", "0.1.0")
  type SummerStormMetrics = storm.option.SummerStormMetrics

  @deprecated("Use com.twitter.summingbird.storm.option.SummerStormMetrics", "0.1.0")
  val SummerStormMetrics = storm.option.SummerStormMetrics

  @deprecated("Use com.twitter.summingbird.storm.option.MaxWaitingFutures", "0.1.0")
  type MaxWaitingFutures = storm.option.MaxWaitingFutures

  @deprecated("Use com.twitter.summingbird.storm.option.MaxWaitingFutures", "0.1.0")
  val MaxWaitingFutures = storm.option.MaxWaitingFutures

  /** Scalding options here */
  @deprecated("Use com.twitter.summingbird.scalding.option.FlatMapShards", "0.1.0")
  val FlatMapShards = scalding.option.FlatMapShards

  @deprecated("Use com.twitter.summingbird.scalding.option.FlatMapShards", "0.1.0")
  type FlatMapShards = scalding.option.FlatMapShards

  @deprecated("Use com.twitter.summingbird.scalding.option.Reducers", "0.1.0")
  val Reducers = scalding.option.Reducers

  @deprecated("Use com.twitter.summingbird.scalding.option.Reducers", "0.1.0")
  type Reducers = scalding.option.Reducers
}
