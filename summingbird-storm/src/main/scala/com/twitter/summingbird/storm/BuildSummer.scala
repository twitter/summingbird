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

import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird.online.OnlineDefaultConstants._
import com.twitter.summingbird.{ Counter, Group, Producer }
import com.twitter.summingbird.online.option._
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

/*
 * The BuildSummer class is responsible for decoding from the options what SummerBuilder to use when setting up bolts.
 * It has two primary modes, reading a SummerConstructor setting directly and using its contents, or via the legacy route.
 * Reading all the options internally.
 */
private[storm] object BuildSummer {
  @transient private[storm] val logger = LoggerFactory.getLogger(BuildSummer.getClass)

  /**
   * @param producer to use to get options for this [[SummerBuilder]].
   */
  def apply(builder: StormTopologyBuilder, nodeName: String, producer: Producer[Storm, _]): SummerBuilder = {
    val summerBuilder = builder.get[SummerConstructor](producer)
      .map { case (_, constructor) => constructor.get }
      .getOrElse {
        logger.info(s"[$nodeName] use legacy way of getting summer builder")
        legacySummerBuilder(builder, producer)
      }

    logger.info(s"[$nodeName] summer builder: $summerBuilder")
    summerBuilder.create { counterName =>
      require(builder.jobId.get != null, "Unable to register metrics with no job id present in the config updater")
      new Counter(Group("summingbird." + nodeName), counterName)(builder.jobId) with Incrementor
    }
  }

  private def legacySummerBuilder(builder: StormTopologyBuilder, producer: Producer[Storm, _]): SummerWithCountersBuilder = {
    def option[T <: AnyRef: ClassTag](default: T): T =
      builder.get[T](producer).map(_._2).getOrElse(default)

    val cacheSize = option(DEFAULT_FM_CACHE)

    if (cacheSize.lowerBound == 0) {
      Summers.Null
    } else {
      val softMemoryFlush = option(DEFAULT_SOFT_MEMORY_FLUSH_PERCENT)
      val flushFrequency = option(DEFAULT_FLUSH_FREQUENCY)
      val useAsyncCache = option(DEFAULT_USE_ASYNC_CACHE)

      if (!useAsyncCache.get) {
        Summers.Sync(cacheSize, flushFrequency, softMemoryFlush)
      } else {
        val asyncPoolSize = option(DEFAULT_ASYNC_POOL_SIZE)
        val valueCombinerCrushSize = option(DEFAULT_VALUE_COMBINER_CACHE_SIZE)
        val doCompact = option(CompactValues.default)
        Summers.Async(
          cacheSize,
          flushFrequency,
          softMemoryFlush,
          asyncPoolSize,
          doCompact,
          valueCombinerCrushSize
        )
      }
    }
  }
}
