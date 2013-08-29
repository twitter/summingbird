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
  val MonoidIsCommutative = option.MonoidIsCommutative
  type MonoidIsCommutative = option.MonoidIsCommutative

  /**
    * Storm flatMap option types and objects.
    */
  type SpoutParallelism = option.SpoutParallelism
  val SpoutParallelism = option.SpoutParallelism
  type FlatMapParallelism = option.FlatMapParallelism
  val FlatMapParallelism = option.FlatMapParallelism
  type FlatMapStormMetrics = option.FlatMapStormMetrics
  val FlatMapStormMetrics = option.FlatMapStormMetrics

  /**
    * Sink objects and types.
    */
  type SinkParallelism = option.SinkParallelism
  val SinkParallelism = option.SinkParallelism
  type OnlineSuccessHandler = option.OnlineSuccessHandler
  val OnlineSuccessHandler = option.OnlineSuccessHandler
  type SinkStormMetrics = option.SinkStormMetrics
  val SinkStormMetrics = option.SinkStormMetrics
  type MaxWaitingFutures = option.MaxWaitingFutures
  val MaxWaitingFutures = option.MaxWaitingFutures
}
