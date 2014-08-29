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

package com.twitter.summingbird.online

import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option._

import com.twitter.util.Duration

/*
 * These are our set of constants that are a base set of sane ones for all online platforms.
 * This shouldn't be directly referred to by user code, hence private[summingbird]. The platform itself
 * should expose its customized set of Constants.
 */
private[summingbird] trait OnlineDefaultConstants {
  val DEFAULT_SOURCE_PARALLELISM = SourceParallelism(1)
  val DEFAULT_FM_PARALLELISM = FlatMapParallelism(5)
  val DEFAULT_FM_CACHE = CacheSize(0)
  val DEFAULT_SUMMER_PARALLELISM = SummerParallelism(5)
  val DEFAULT_ONLINE_SUCCESS_HANDLER = OnlineSuccessHandler(_ => {})
  val DEFAULT_ONLINE_EXCEPTION_HANDLER = OnlineExceptionHandler(Map.empty)
  val DEFAULT_SUMMER_CACHE = CacheSize(0)
  val DEFAULT_MONOID_IS_COMMUTATIVE = MonoidIsCommutative.default
  val DEFAULT_MAX_WAITING_FUTURES = MaxWaitingFutures(10)
  val DEFAULT_MAX_FUTURE_WAIT_TIME = MaxFutureWaitTime(Duration.fromSeconds(60))
  val DEFAULT_FLUSH_FREQUENCY = FlushFrequency(Duration.fromSeconds(10))
  val DEFAULT_USE_ASYNC_CACHE = UseAsyncCache(false)
  val DEFAULT_ASYNC_POOL_SIZE = AsyncPoolSize(Runtime.getRuntime().availableProcessors())
  val DEFAULT_SOFT_MEMORY_FLUSH_PERCENT = SoftMemoryFlushPercent(80.0F)
  val DEFAULT_VALUE_COMBINER_CACHE_SIZE = ValueCombinerCacheSize(100)
  val DEFAULT_MAX_EMIT_PER_EXECUTE = MaxEmitPerExecute(Int.MaxValue)
  val DEFAULT_SUMMER_BATCH_MULTIPLIER = SummerBatchMultiplier(1)
}

private[summingbird] object OnlineDefaultConstants extends OnlineDefaultConstants