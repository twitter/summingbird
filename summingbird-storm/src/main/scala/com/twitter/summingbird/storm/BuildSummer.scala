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

import com.twitter.algebird.util.summer._
import com.twitter.summingbird.online.OnlineDefaultConstants._
import com.twitter.summingbird.{ Counter, Group, Name }
import com.twitter.summingbird.online.option.{ CompactValues, SummerBuilder, SummerConstructor, Summers }
import com.twitter.summingbird.storm.planner.StormNode
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

/*
 * The BuildSummer class is responsible for decoding from the options what SummerBuilder to use when setting up bolts.
 * It has two primary modes, reading a SummerConstructor setting directly and using its contents, or via the legacy route.
 * Reading all the options internally.
 */
private[storm] object BuildSummer {
  @transient private[storm] val logger = LoggerFactory.getLogger(BuildSummer.getClass)

  def apply(builder: StormTopologyBuilder, node: StormNode): SummerBuilder = {
    val summerConstructor = builder.get[SummerConstructor](node)
      .map { case (_, constructor) => constructor }.getOrElse {
      logger.debug(s"Node (${builder.getNodeName(node)}): Use legacy way of getting summer constructor.")
      legacySummerConstructor(builder, node)
    }

    logger.debug(s"Node (${builder.getNodeName(node)}): Use $summerConstructor as summer constructor.")
    summerConstructor.get.apply(NodeContext(builder, node))
  }

  private def legacySummerConstructor(builder: StormTopologyBuilder, node: StormNode): SummerConstructor = {
    def option[T <: AnyRef: ClassTag](default: T): T = builder.getOrElse[T](node, default)

    val cacheSize = option(DEFAULT_FM_CACHE)

    if (cacheSize.lowerBound == 0) {
      Summers.Null
    } else {
      val softMemoryFlush = option(DEFAULT_SOFT_MEMORY_FLUSH_PERCENT)
      val flushFrequency = option(DEFAULT_FLUSH_FREQUENCY)
      val useAsyncCache = option(DEFAULT_USE_ASYNC_CACHE)

      if (!useAsyncCache.get) {
        Summers.sync(cacheSize, flushFrequency, softMemoryFlush)
      } else {
        val asyncPoolSize = option(DEFAULT_ASYNC_POOL_SIZE)
        val valueCombinerCrushSize = option(DEFAULT_VALUE_COMBINER_CACHE_SIZE)
        val doCompact = option(CompactValues.default)
        Summers.async(
          cacheSize, flushFrequency, softMemoryFlush, asyncPoolSize, doCompact, valueCombinerCrushSize
        )
      }
    }
  }

  private case class NodeContext(builder: StormTopologyBuilder, node: StormNode) extends SummerConstructor.Context {
    override def counter(name: Name): Counter with Incrementor = {
      require(builder.jobId.get != null, "Unable to register metrics with no job id present in the config updater")
      new Counter(Group("summingbird." + builder.getNodeName(node)), name)(builder.jobId) with Incrementor
    }
  }
}
