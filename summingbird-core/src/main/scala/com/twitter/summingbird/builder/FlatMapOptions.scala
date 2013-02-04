package com.twitter.summingbird.builder

import com.twitter.summingbird.storm.StormMetric

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Options used by the FlatMappedBuilder.
 */

sealed trait FlatMapOption extends java.io.Serializable
case class FlatMapShards(count: Int) extends FlatMapOption
case class FlatMapParallelism(parHint: Int) extends FlatMapOption

// This stupidity is necessary because val parameters can't be call-by-name.
// We pass a function so that the metrics aren't serialized. Beyond the storm IMetric not being
// serializable, passing a value also causes problems with the instance registered in the bolt
// being different from the one used in the summingbird job.
object FlatMapStormMetrics {
  def apply(metrics: => TraversableOnce[StormMetric[_]]) = new FlatMapStormMetrics(() => metrics)
  def unapply(metrics: FlatMapStormMetrics) = Some(metrics.metrics)
}
class FlatMapStormMetrics(val metrics: () => TraversableOnce[StormMetric[_]]) extends FlatMapOption
