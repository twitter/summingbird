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
import backtype.storm.metric.api.IMetric
import com.twitter.tormenta.spout.Metric

/**
 * Options used by the flatMapping stage of a storm topology.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

case class SpoutParallelism(parHint: Int)

case class FlatMapParallelism(parHint: Int)

/**
  * This stupidity is necessary because val parameters can't be
  * call-by-name.  We pass a function so that the metrics aren't
  * serialized. Beyond the storm IMetric not being serializable,
  * passing a value also causes problems with the instance registered
  * in the bolt being different from the one used in the summingbird
  * job.
  */
object FlatMapStormMetrics {
  def apply(metrics: => TraversableOnce[StormMetric[IMetric]]) = new FlatMapStormMetrics(() => metrics)
  def unapply(metrics: FlatMapStormMetrics) = Some(metrics.metrics)
}

class FlatMapStormMetrics(val metrics: () => TraversableOnce[StormMetric[IMetric]])


object SpoutStormMetrics {
  def apply(metrics: => TraversableOnce[StormMetric[IMetric]]) = new SpoutStormMetrics(() => metrics)
  def unapply(metrics: SpoutStormMetrics) = Some(metrics.metrics)
}

class SpoutStormMetrics(val metrics: () => TraversableOnce[StormMetric[IMetric]]) {
  def toSpoutMetrics: () => TraversableOnce[Metric[IMetric]] =
    {() => metrics().map{ x: StormMetric[IMetric] => Metric(x.name, x.metric, x.interval.inSeconds)}}
}