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

package com.twitter.summingbird.akka

import com.twitter.summingbird.option.MonoidIsCommutative
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option._

import com.twitter.util.Duration

object Constants {
  val AGG_KEY     = "aggKey"
  val AGG_VALUE   = "aggValue"
  val AGG_BATCH   = "aggBatchID"
  val RETURN_INFO = "return-info"

  val VALUE_FIELD = "value"
  val GROUP_BY_SUM = "groupBySum"

  val DEFAULT_FM_CACHE = CacheSize(0)
  val DEFAULT_ONLINE_SUCCESS_HANDLER = OnlineSuccessHandler(_ => {})
  val DEFAULT_ONLINE_EXCEPTION_HANDLER = OnlineExceptionHandler(Map.empty)
  val DEFAULT_SUMMER_CACHE = CacheSize(0)
  val DEFAULT_MONOID_IS_COMMUTATIVE = MonoidIsCommutative.default
  val DEFAULT_MAX_WAITING_FUTURES = MaxWaitingFutures(10)
  val DEFAULT_MAX_FUTURE_WAIT_TIME =  MaxFutureWaitTime(Duration.fromSeconds(60))
  val DEFAULT_FLUSH_FREQUENCY =  FlushFrequency(Duration.fromSeconds(40))
}
