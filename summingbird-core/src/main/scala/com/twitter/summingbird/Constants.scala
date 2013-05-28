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

import com.twitter.summingbird.builder._
import com.twitter.summingbird.util.CacheSize

object Constants {
  val AGG_KEY     = "aggKey"
  val AGG_VALUE   = "aggValue"
  val AGG_BATCH   = "aggBatchID"
  val RETURN_INFO = "return-info"

  val VALUE_FIELD = "value"
  val RPC_STREAM = "rpc-result"
  val DRPC_SPOUT = "drpc-spout"
  val DRPC_DECODER = "drpcDecoder"
  val GROUP_BY_SUM = "groupBySum"
  val RPC_RETURN = "rpcReturn"

  val DEFAULT_FM_PARALLELISM = FlatMapParallelism(5)
  val DEFAULT_FM_SHARDS = FlatMapShards(0)
  val DEFAULT_FM_STORM_METRICS = FlatMapStormMetrics(None)
  val DEFAULT_FM_CACHE = CacheSize(0)
  val DEFAULT_RPC_PARALLELISM = RpcParallelism(1)
  val DEFAULT_DECODER_PARALLELISM = DecoderParallelism(1)
  val DEFAULT_SINK_PARALLELISM = SinkParallelism(5)
  val DEFAULT_ONLINE_SUCCESS_HANDLER = OnlineSuccessHandler(_ => {})
  val DEFAULT_ONLINE_EXCEPTION_HANDLER = OnlineExceptionHandler(Map.empty)
  val DEFAULT_SINK_CACHE = CacheSize(0)
  val DEFAULT_SINK_STORM_METRICS = SinkStormMetrics(None)
  val DEFAULT_MONOID_IS_COMMUTATIVE = MonoidIsCommutative(false)
  val DEFAULT_MAX_WAITING_FUTURES = MaxWaitingFutures(10)
}
