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

import java.io.Serializable

import com.twitter.util.Duration
import org.apache.storm.metric.api.IMetric
import org.apache.storm.task.TopologyContext

/**
 * Necessary info for registering a metric in storm
 * "interval" is period over which the metric will be aggregated
 *
 * @author Ashutosh Singhal
 */

case class StormMetric[+T <: IMetric](metric: T, name: String, interval: Duration) {
  def register(context: TopologyContext) {
    context.registerMetric(name, metric, interval.inSeconds)
  }
}
