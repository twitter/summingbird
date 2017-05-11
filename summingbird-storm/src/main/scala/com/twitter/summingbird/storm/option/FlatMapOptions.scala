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
import com.twitter.tormenta.spout.Metric
import java.io.Serializable
import org.apache.storm.metric.api.IMetric

/**
 * Options used by the flatMapping stage of a storm topology.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

/**
 * This workaround is necessary because val parameters can't be
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

/**
 * When a bolt is prepared, these metrics will be use by being called with the TopologyContext for the storm
 * bolt.
 */
class FlatMapStormMetrics(val metrics: () => TraversableOnce[StormMetric[IMetric]])

object SpoutStormMetrics {
  def apply(metrics: => TraversableOnce[StormMetric[IMetric]]) = new SpoutStormMetrics(() => metrics)
  def unapply(metrics: SpoutStormMetrics) = Some(metrics.metrics)
}

class SpoutStormMetrics(val metrics: () => TraversableOnce[StormMetric[IMetric]]) extends Serializable {
  def toSpoutMetrics: () => TraversableOnce[Metric[IMetric]] =
    { () => metrics().map { x: StormMetric[IMetric] => Metric(x.name, x.metric, x.interval.inSeconds) } }
}

/**
 * This signals that the storm bolts should use localOrShuffleGrouping, which means that if the downstream bolt
 * has a task on the same local worker, the output will only go to those tasks. Otherwise, shuffling
 * happens normally. This is important to understand as this can create hot spots in the topology.
 */
case class PreferLocalDependency(get: Boolean)

/**
 * If this is set to true, this means that a bolt will ack a tuple as soon as it is received and processing begins;
 * otherwise, the tuple will be acked when the bolt completes. Acking signals to storm that a tuple has been fully
 * processed, so if a tuple is acked on entry and then there is a failure it will not be replayed per storm's
 * normal replay mechanisms.
 */
case class AckOnEntry(get: Boolean)

/**
 * Maximum number of elements to execute in a given second per task
 */
case class MaxExecutePerSecond(lowerBound: Long, upperBound: Long, rampUptimeMS: Long) {
  require(rampUptimeMS >= 0L, "Ramp up time must greater than or equal to zero")
}