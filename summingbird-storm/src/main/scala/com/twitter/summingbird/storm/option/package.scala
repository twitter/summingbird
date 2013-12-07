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

package com.twitter.summingbird.storm

import com.twitter.summingbird
import com.twitter.summingbird.online


package object option {
  @deprecated("Use com.twitter.summingbird.option.CacheSize", "0.2.5")
  type CacheSize = summingbird.option.CacheSize

  @deprecated("Use com.twitter.summingbird.option.CacheSize", "0.2.5")
  val CacheSize = summingbird.option.CacheSize

  @deprecated("Use com.twitter.summingbird.option.SummerParallelism", "0.3.0")
  type SinkParallelism = SummerParallelism
  val SinkParallelism = SummerParallelism

  @deprecated("Use com.twitter.summingbird.option.SummerStormMetrics", "0.3.0")
  type SinkStormMetrics = SummerStormMetrics
  val SinkStormMetrics = SummerStormMetrics

  @deprecated("Use com.twitter.summingbird.online.option.OnlineSuccessHandler", "0.2.6")
  type OnlineSuccessHandler = online.option.OnlineSuccessHandler
  val OnlineSuccessHandler = online.option.OnlineSuccessHandler


  @deprecated("Use com.twitter.summingbird.online.option.IncludeSuccessHandler", "0.2.6")
  type IncludeSuccessHandler = online.option.IncludeSuccessHandler
  val IncludeSuccessHandler = online.option.IncludeSuccessHandler

  @deprecated("Use com.twitter.summingbird.online.option.OnlineExceptionHandler", "0.2.6")
  type OnlineExceptionHandler = online.option.OnlineExceptionHandler
  val OnlineExceptionHandler = online.option.OnlineExceptionHandler

  @deprecated("Use com.twitter.summingbird.online.option.MaxWaitingFutures", "0.2.6")
  type MaxWaitingFutures = online.option.MaxWaitingFutures
  val MaxWaitingFutures = online.option.MaxWaitingFutures

}
