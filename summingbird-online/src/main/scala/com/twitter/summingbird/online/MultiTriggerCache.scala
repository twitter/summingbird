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
import com.twitter.summingbird.online.option.{AsyncPoolSize, FlushFrequency}
import java.util.concurrent._
import scala.collection.JavaConversions._
import com.twitter.util.Future
import scala.collection.mutable.{Set => MSet, Queue => MQueue, Map => MMap}
import com.twitter.util.FuturePool

import org.slf4j.{LoggerFactory, Logger}

/**
 * @author Ian O Connell
 */

object MultiTriggerCache {
  def builder[Key, Value](cacheSize: CacheSize, flushFrequency: FlushFrequency, poolSize: AsyncPoolSize) =
      {(sg: Semigroup[Value]) =>
            new MultiTriggerCache[Key, Value](cacheSize, flushFrequency, poolSize)(sg) }
}

case class MultiTriggerCache[Key, Value](cacheSizeOpt: CacheSize, flushFrequency: FlushFrequency, poolSize: AsyncPoolSize)
  (implicit monoid: Semigroup[Value]) extends AsyncCache[Key, Value] {

  private val (executor, pool) = if(poolSize.get > 0) {
      val executor = new ThreadPoolExecutor(poolSize.get, poolSize.get, 60, TimeUnit.SECONDS, new LinkedBlockingQueue(1000))
      (Some(executor), FuturePool(executor))
    } else {
      (None, FuturePool.immediatePool)
    }

  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  private val cacheSize = cacheSizeOpt.size.getOrElse(0)
  private val keyMap = new ConcurrentHashMap[Key, List[Value]]()
  @volatile private var lastDump:Long = System.currentTimeMillis

  private lazy val runtime  = Runtime.getRuntime
  private def memoryWaterMark = {
    val used = (runtime.totalMemory - runtime.freeMemory).toDouble / runtime.maxMemory
    used > 0.8
  }

  private def timedOut = (System.currentTimeMillis - lastDump) > flushFrequency.get.inMilliseconds
  private def keySpaceTooBig = keyMap.size > cacheSize

  override def cleanup {
    executor.map{e =>
      e.shutdown
    }
    super.cleanup
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
      valList.map{case (k, v) => merge(k, v) }
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
  private def merge(key: Key, extraVals: List[Value]) {
    val oldValue = Option(keyMap.remove(key)).getOrElse(List[Value]())
    val newVal = extraVals ::: oldValue
    val mutated = if(newVal.size > cacheSize) {
      List(monoid.sumOption(newVal).get)
    } else newVal
    if(keyMap.putIfAbsent(key, mutated) != null) {
      merge(key, mutated)
    }
  }

  private def merge(key: Key, extraValue: Value): Unit = merge(key, List(extraValue))

  private def doFlushCache: Map[Key, Value] = {
    val startKeyset: Set[Key] = keyMap.keySet.toSet
    lastDump = System.currentTimeMillis
    startKeyset.flatMap{case k =>
      Option(keyMap.remove(k)).map(monoid.sumOption(_).get).map((k, _))
    }.toMap
  }
}
