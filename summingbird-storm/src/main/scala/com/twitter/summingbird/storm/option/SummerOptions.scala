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
  * SinkParallelism controls the number of executors storm allocates to
  * the groupAndSum bolts. Each of these bolt executors is responsible
  * for storing and committing some subset of the keyspace to the
  * Sink's store, so higher parallelism will result in higher load on
  * the store. The default sink parallelism is 5.
  */
case class SummerParallelism(parHint: Int)

case class OnlineSuccessHandler(handlerFn: Unit => Unit)

/**
  * Kryo serialization problems have been observed with using
  * OnlineSuccessHandler. This enables easy disabling of the handler.
  * TODO (https://github.com/twitter/summingbird/issues/82): remove
  * once we know what the hell is going on with this
  */
case class IncludeSuccessHandler(get: Boolean)

object IncludeSuccessHandler {
  val default = IncludeSuccessHandler(true)
}

case class OnlineExceptionHandler(handlerFn: PartialFunction[Throwable, Unit])

/**
  * See FlatMapOptions.scala for an explanation.
  */
object SummerStormMetrics {
  def apply(metrics: => TraversableOnce[StormMetric[_]]) = new SummerStormMetrics(() => metrics)
  def unapply(metrics: SummerStormMetrics) = Some(metrics.metrics)
}
class SummerStormMetrics(val metrics: () => TraversableOnce[StormMetric[_]])

/**
  * MaxWaitingFutures is the maximum number of key-value pairs that the
  * SinkBolt in Storm will process before starting to force the
  * futures. For example, setting MaxWaitingFutures(100) means that if
  * a key-value pair is added to the OnlineStore and the (n - 100)th
  * write has not completed, Storm will block before moving on to the
  * next key-value pair.

  * TODO (https://github.com/twitter/summingbird/issues/83): look into
  * removing this due to the possibility of deadlock with the sink's
  * cache.
  */
case class MaxWaitingFutures(get: Int)

/**
 * All futures should return in a reasonable period of time, otherwise
 * there will be memory issues keeping all of them open. This option is
 * to set the longest we wait on a future. It is not a substitute for correctly
 * configured and implemented stores, services and sinks. All of those should
 * return or fail fairly quickly (on the order of a second or so).
 */
case class MaxFutureWaitTime(get: Duration)

/**
 * All futures should return in a reasonable period of time, otherwise
 * there will be memory issues keeping all of them open. This option is
 * to set the longest we wait on a future. It is not a substitute for correctly
 * configured and implemented stores, services and sinks. All of those should
 * return or fail fairly quickly (on the order of a second or so).
 */
case class FlushFrequency(get: Duration)