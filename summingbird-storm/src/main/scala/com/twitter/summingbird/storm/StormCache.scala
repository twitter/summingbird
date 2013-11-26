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

import com.twitter.algebird.{ SummingQueue, Semigroup, MapAlgebra }
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.batch.{ Batcher, BatchID, Timestamp}
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.util.{Return, Throw}
import com.twitter.storehaus.algebra.SummerConstructor
import com.twitter.summingbird.option.CacheSize
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.storm.option.FlushFrequency

import com.twitter.util.{Future}

import MergeableStore.enrich

import scala.collection.breakOut
import scala.collection.mutable.{Queue => MQueue, Map => MMap}

import org.slf4j.{LoggerFactory, Logger}

/**
 * @author Ian O COnnell
 */
case class StormCache[Key, Value](cacheSizeOpt: CacheSize, flushFrequency: FlushFrequency)
  (implicit monoid: Semigroup[Value]) {

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

  def tick: Option[Map[Key, Value]] = {
    innerTick
  }

  private def innerTick: Option[Map[Key, Value]] = {
    logger.debug("Timedout {}", timedOut)
    logger.debug("keySpaceTooBig {}", keySpaceTooBig)
    logger.debug("memoryWaterMark {}", memoryWaterMark)
    if (timedOut || keySpaceTooBig || memoryWaterMark) {
        Some(doFlushCache)
      }
    else {
      None
    }
  }

  def insert(vals: TraversableOnce[(Key, Value)]): Option[Map[Key, Value]] = {
    //val resMap = MapAlgebra.sumByKey(vals)

    this.synchronized {
      vals.map {case (k, v) =>
        keyMap.get(k) match {
          case Some(vQueue) =>
            vQueue += v
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

  def doFlushCache: Map[Key, Value] = {
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
