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

import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer._
import com.twitter.summingbird.{ Name, Group, Counter }
import com.twitter.summingbird.online.option.{ SummerBuilder, SummerConstructor }
import com.twitter.summingbird.option.{ JobId, CacheSize }
import com.twitter.summingbird.planner.Dag
import com.twitter.summingbird.storm.planner.StormNode
import com.twitter.util.{ Future, FuturePool }

import java.util.concurrent.{ Executors, TimeUnit }

import org.slf4j.LoggerFactory

import Constants._

/*
 * The BuildSummer class is responsible for decoding from the options what SummerBuilder to use when setting up bolts.
 * It has two primary modes, reading a SummerConstructor setting directly and using its contents, or via the legacy route.
 * Reading all the options internally.
 */
object BuildSummer {
  @transient private val logger = LoggerFactory.getLogger(BuildSummer.getClass)

  def apply(storm: Storm, dag: Dag[Storm], node: StormNode, jobID: JobId) = {
    val opSummerConstructor = storm.get[SummerConstructor](dag, node).map(_._2)
    logger.debug(s"Node (${dag.getNodeName(node)}): Queried for SummerConstructor, got $opSummerConstructor")

    opSummerConstructor match {
      case Some(cons) =>
        logger.debug(s"Node (${dag.getNodeName(node)}): Using user supplied SummerConstructor: $cons")
        cons.get
      case None => legacyBuilder(storm, dag, node, jobID)
    }
  }

  private[this] final def legacyBuilder(storm: Storm, dag: Dag[Storm], node: StormNode, jobID: JobId) = {
    val nodeName = dag.getNodeName(node)
    val cacheSize = storm.getOrElse(dag, node, DEFAULT_FM_CACHE)
    require(jobID.get != null, "Unable to register metrics with no job id present in the config updater")
    logger.info(s"[$nodeName] cacheSize lowerbound: ${cacheSize.lowerBound}")

    val memoryCounter = counter(jobID, Group(nodeName), Name("memory"))
    val timeoutCounter = counter(jobID, Group(nodeName), Name("timeout"))
    val sizeCounter = counter(jobID, Group(nodeName), Name("size"))
    val tupleInCounter = counter(jobID, Group(nodeName), Name("tuplesIn"))
    val tupleOutCounter = counter(jobID, Group(nodeName), Name("tuplesOut"))
    val insertCounter = counter(jobID, Group(nodeName), Name("inserts"))
    val insertFailCounter = counter(jobID, Group(nodeName), Name("insertFail"))

    if (cacheSize.lowerBound == 0) {
      new SummerBuilder {
        def getSummer[K, V: Semigroup]: com.twitter.algebird.util.summer.AsyncSummer[(K, V), Map[K, V]] = {
          new com.twitter.algebird.util.summer.NullSummer[K, V](tupleInCounter, tupleOutCounter)
        }
      }
    } else {
      val softMemoryFlush = storm.getOrElse(dag, node, DEFAULT_SOFT_MEMORY_FLUSH_PERCENT)
      logger.info(s"[$nodeName] softMemoryFlush : ${softMemoryFlush.get}")

      val flushFrequency = storm.getOrElse(dag, node, DEFAULT_FLUSH_FREQUENCY)
      logger.info(s"[$nodeName] maxWaiting: ${flushFrequency.get}")

      val useAsyncCache = storm.getOrElse(dag, node, DEFAULT_USE_ASYNC_CACHE)
      logger.info(s"[$nodeName] useAsyncCache : ${useAsyncCache.get}")

      if (!useAsyncCache.get) {
        new SummerBuilder {
          def getSummer[K, V: Semigroup]: com.twitter.algebird.util.summer.AsyncSummer[(K, V), Map[K, V]] = {
            new SyncSummingQueue[K, V](
              BufferSize(cacheSize.lowerBound),
              FlushFrequency(flushFrequency.get),
              MemoryFlushPercent(softMemoryFlush.get),
              memoryCounter,
              timeoutCounter,
              sizeCounter,
              insertCounter,
              tupleInCounter,
              tupleOutCounter)
          }
        }
      } else {
        val asyncPoolSize = storm.getOrElse(dag, node, DEFAULT_ASYNC_POOL_SIZE)
        logger.info(s"[$nodeName] asyncPoolSize : ${asyncPoolSize.get}")

        val valueCombinerCrushSize = storm.getOrElse(dag, node, DEFAULT_VALUE_COMBINER_CACHE_SIZE)
        logger.info(s"[$nodeName] valueCombinerCrushSize : ${valueCombinerCrushSize.get}")

        new SummerBuilder {
          def getSummer[K, V: Semigroup]: com.twitter.algebird.util.summer.AsyncSummer[(K, V), Map[K, V]] = {
            val executor = Executors.newFixedThreadPool(asyncPoolSize.get)
            val futurePool = FuturePool(executor)
            val summer = new AsyncListSum[K, V](BufferSize(cacheSize.lowerBound),
              FlushFrequency(flushFrequency.get),
              MemoryFlushPercent(softMemoryFlush.get),
              memoryCounter,
              timeoutCounter,
              insertCounter,
              insertFailCounter,
              sizeCounter,
              tupleInCounter,
              tupleOutCounter,
              futurePool,
              Compact(false),
              CompactionSize(0))
            summer.withCleanup(() => {
              Future {
                executor.shutdown
                executor.awaitTermination(10, TimeUnit.SECONDS)
              }
            })
          }
        }
      }
    }
  }

  def counter(jobID: JobId, nodeName: Group, counterName: Name) = new Counter(Group("summingbird." + nodeName.getString), counterName)(jobID) with Incrementor

}
