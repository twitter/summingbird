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

package com.twitter.summingbird.builder

import com.twitter.summingbird.storm.StormMetric

/**
 * Options used by the FlatMappedBuilder.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

sealed trait FlatMapOption extends java.io.Serializable

case class SpoutParallelism(parHint: Int) extends FlatMapOption

case class FlatMapParallelism(parHint: Int) extends FlatMapOption

// This stupidity is necessary because val parameters can't be
// call-by-name.  We pass a function so that the metrics aren't
// serialized. Beyond the storm IMetric not being serializable,
// passing a value also causes problems with the instance registered
// in the bolt being different from the one used in the summingbird
// job.
object FlatMapStormMetrics {
  def apply(metrics: => TraversableOnce[StormMetric[_]]) = new FlatMapStormMetrics(() => metrics)
  def unapply(metrics: FlatMapStormMetrics) = Some(metrics.metrics)
}

class FlatMapStormMetrics(val metrics: () => TraversableOnce[StormMetric[_]]) extends FlatMapOption
