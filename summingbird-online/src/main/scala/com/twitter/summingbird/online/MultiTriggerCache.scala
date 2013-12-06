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

package com.twitter.summingbird.online

import com.twitter.algebird.{ Semigroup, MapAlgebra }
import com.twitter.util.{Return, Throw}
import com.twitter.summingbird.option.CacheSize
import scala.collection.mutable.SynchronizedQueue
import com.twitter.summingbird.online.option.{ValueCombinerCacheSize, AsyncPoolSize, FlushFrequency, SoftMemoryFlushPercent}
import java.util.concurrent.{Executors, ConcurrentHashMap, TimeUnit}
import scala.collection.JavaConverters._
import com.twitter.util.Future
import scala.collection.mutable.{Set => MSet, Map => MMap}
import com.twitter.util.FuturePool
import scala.collection.breakOut

import org.slf4j.{LoggerFactory, Logger}

/**
 * @author Ian O Connell
 */

object MultiTriggerCache {
  def builder[Key, Value](cacheSize: CacheSize, valueCombinerCacheSize: ValueCombinerCacheSize, flushFrequency: FlushFrequency, softMemoryFlush: SoftMemoryFlushPercent, poolSize: AsyncPoolSize) =
      {(sg: Semigroup[Value]) =>
            new MultiTriggerCache[Key, Value](cacheSize, valueCombinerCacheSize, flushFrequency, softMemoryFlush, poolSize)(sg) }
}

case class MultiTriggerCache[Key, Value](cacheSizeOpt: CacheSize, valueCombinerCacheSize: ValueCombinerCacheSize, flushFrequency: FlushFrequency, softMemoryFlush: SoftMemoryFlushPercent, poolSize: AsyncPoolSize)
  (implicit semigroup: Semigroup[Value]) extends AsyncCache[Key, Value] {

  private lazy val (executor, pool) = if(poolSize.get > 0) {
      val executor = Executors.newFixedThreadPool(poolSize.get)
      (Some(executor), FuturePool(executor))
    } else {
      (None, FuturePool.immediatePool)
    }

  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  private val cacheSize = cacheSizeOpt.size.getOrElse(0)

  private val keyMap = new ConcurrentHashMap[Key, Queue[Value]]()
  @volatile private var lastDump:Long = System.currentTimeMillis

  private lazy val runtime  = Runtime.getRuntime
    private def memoryWaterMark = {
    val used = ((runtime.totalMemory - runtime.freeMemory).toDouble * 100) / runtime.maxMemory
    used > softMemoryFlush.get
  }

  private def timedOut = (System.currentTimeMillis - lastDump) >= flushFrequency.get.inMilliseconds
  private def keySpaceTooBig = keyMap.size > cacheSize

  override def cleanup = {
    Future {
      executor.map{e =>
        e.shutdown
        e.awaitTermination(10, TimeUnit.SECONDS)
      }
    }.flatMap(f => super.cleanup)
  }

  def forceTick: Future[Map[Key, Value]] = {
    pool {
      doFlushCache
    }
  }

  def tick: Future[Map[Key, Value]] = {
    if (timedOut || keySpaceTooBig || memoryWaterMark) {
        pool {
          doFlushCache
        }
      }
    else {
      Future.value(Map.empty)
    }
  }

  def insert(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    val valList = vals.toList
    pool {
      valList.foreach {case (k, v) =>
        if(k == null || v == null) {
          throw new Exception("Unable to store nulls in cache")
        }
        merge(k, List(v)) }
      innerTick
    }
  }

  // All internal functions from here down, should be private
  // And unable to use the pool

  private def innerTick: Map[Key, Value] = {
    if (timedOut || keySpaceTooBig || memoryWaterMark) {
        doFlushCache
      }
    else {
      Map.empty
    }
  }

  @annotation.tailrec
  private def merge(key: Key, extraVals: TraversableOnce[Value]) {
    val oldQueue = Option(keyMap.get(key)).getOrElse(Queue.linkedNonBlocking[Value])
    oldQueue.putAll(extraVals)

    // We have a high locality  for a single tuple, crush it down
    // If this is triggered we will go back around with an in memory list of size 1
    if(oldQueue.size > valueCombinerCacheSize.get) {
      val dataCP = oldQueue.toSeq
      if(dataCP.size > 0) {
        merge(key, List(semigroup.sumOption(dataCP).get))
      }
    }
    // Otherwise, we attempt to update the concurrent hash map
    else {
      // This will return null if the key was present before
      // We terminate with the merge complete if its == null.
      if(keyMap.putIfAbsent(key, oldQueue) != null) {
        // If its not null then we test to make sure we are the queue present
        if(!(keyMap.get(key) == oldQueue)) {
          // Guard against needlessly inserting if we have been drained in parallel by another actor
          val orphanValues = oldQueue.toSeq
          if(orphanValues.size > 0) {
            merge(key, orphanValues)
          }
        }
      }
    }
  }

  private def doFlushCache: Map[Key, Value] = {
    val startKeyset: Set[Key] = keyMap.keySet.asScala.toSet
    lastDump = System.currentTimeMillis
    startKeyset.flatMap{ k =>
      Option(keyMap.remove(k)).map(_.toSeq).flatMap(semigroup.sumOption(_)).map((k, _))
    }(breakOut)
  }
}
