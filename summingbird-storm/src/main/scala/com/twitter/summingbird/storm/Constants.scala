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

import com.twitter.summingbird.storm.option._
import com.twitter.summingbird.online.OnlineDefaultConstants

/**
 * These are the storm specific constants.
 * Here we can override ones from online, or add more that are more Storm specific
 */
object Constants extends OnlineDefaultConstants {
  val AGG_KEY = "aggKey"
  val AGG_VALUE = "aggValue"
  val AGG_BATCH = "aggBatchID"
  val RETURN_INFO = "return-info"

  val VALUE_FIELD = "value"
  val GROUP_BY_SUM = "groupBySum"

  val DEFAULT_SPOUT_STORM_METRICS = SpoutStormMetrics(None)
  val DEFAULT_FM_STORM_METRICS = FlatMapStormMetrics(None)
  val DEFAULT_SUMMER_STORM_METRICS = SummerStormMetrics(None)
  val DEFAULT_FM_PREFER_LOCAL_DEPENDENCY = PreferLocalDependency(false)
  val DEFAULT_ACK_ON_ENTRY = AckOnEntry(false)
  val DEFAULT_MAX_EXECUTE_PER_SEC = MaxExecutePerSecond(lowerBound = Int.MaxValue, upperBound = Int.MaxValue, rampUptimeMS = 0L)
}
