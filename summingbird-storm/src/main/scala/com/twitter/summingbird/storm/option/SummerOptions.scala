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

package com.twitter.summingbird.storm.option

import com.twitter.summingbird.storm.StormMetric

import com.twitter.util.Duration

/**
 * Options used by the sink phase of the Storm Platform's topology.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

/**
 * See FlatMapOptions.scala for an explanation.
 */
object SummerStormMetrics {
  def apply(metrics: => TraversableOnce[StormMetric[_]]) = new SummerStormMetrics(() => metrics)
  def unapply(metrics: SummerStormMetrics) = Some(metrics.metrics)
}
class SummerStormMetrics(val metrics: () => TraversableOnce[StormMetric[_]])
