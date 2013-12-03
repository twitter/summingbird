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
import com.twitter.summingbird.online.option.FlushFrequency
import java.util.concurrent._
import com.twitter.util.Future
import scala.collection.mutable.{Queue => MQueue, Map => MMap}
import com.twitter.util.FuturePool

import org.slf4j.{LoggerFactory, Logger}

/**
 * @author Ian O Connell
 */
case class MultiTriggerCache[Key, Value](cacheSizeOpt: CacheSize, flushFrequency: FlushFrequency)
  (implicit monoid: Semigroup[Value]) {

  val executor = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue(1000))
  val pool     =  FuturePool(executor) //FuturePool.immediatePool

  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  private val cacheSize = cacheSizeOpt.size.getOrElse(0)
  @volatile private var keyMap = MMap[Key, MQueue[Value]]()
  @volatile private var lastDump:Long = System.currentTimeMillis

  private lazy val runtime  = Runtime.getRuntime
  private def memoryWaterMark = {
    val used = (runtime.totalMemory - runtime.freeMemory).toDouble / runtime.maxMemory
    used > 0.8
  }

  private def timedOut = (System.currentTimeMillis - lastDump) > flushFrequency.get.inMilliseconds
  private def keySpaceTooBig = keyMap.size > cacheSize

  def tick: Future[Map[Key, Value]] = {
    if (timedOut || keySpaceTooBig || memoryWaterMark) {
        pool {
          innerTick
        }
      }
    else {
      Future.value(Map.empty)
    }
  }

  private def innerTick: Map[Key, Value] = {
    if (timedOut || keySpaceTooBig || memoryWaterMark) {
        doFlushCache
    }
    else {
      Map()
    }
  }


  def insert(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    pool {
      val valList = vals.toList
      this.synchronized {
        valList.map {case (k, v) =>
          keyMap.get(k) match {
            case Some(vQueue) => vQueue += v
              if(vQueue.size > cacheSize && vQueue.size > 1) {
                  val newV = monoid.sumOption(vQueue).get
                  vQueue.clear
                  vQueue += newV
                }
            case None => keyMap.put(k, MQueue(v))
          }
        }
      }
      innerTick
    }
  }

  private def doFlushCache: Map[Key, Value] = {

    val oldKeyMap = this.synchronized {
      val oldKeyMap = keyMap
      keyMap = MMap[Key, MQueue[Value]]()
      lastDump = System.currentTimeMillis
      oldKeyMap
    }

    // The get is valid since by construction we must have elements in the value
    oldKeyMap.mapValues(monoid.sumOption(_).get).toMap
  }
}
