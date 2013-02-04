package com.twitter.summingbird.storm

import java.io.Serializable

import backtype.storm.task.TopologyContext

import com.twitter.util.Duration
import backtype.storm.metric.api.IMetric

/**
 * Necessary info for registering a metric in storm
 * "interval" is period over which the metric will be aggregated
 * @author Ashutosh Singhal
 */

case class StormMetric[T <: IMetric](metric: T, name: String, interval: Duration) {
  def register(context: TopologyContext) {
    context.registerMetric(name, metric, interval.inSeconds)
  }
}
