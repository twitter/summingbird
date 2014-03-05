/*
Copyright 2012 Twitter, Inc.

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

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.option.CacheSize
import com.twitter.util.{Future, Promise, FuturePool, Await}
import com.twitter.summingbird.online.option.{FlushFrequency, SoftMemoryFlushPercent}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.concurrent._
import org.slf4j.{LoggerFactory, Logger}

object BackroundCompactionCache {
  def builder[Key, Value](cacheSize: CacheSize, flushFrequency: FlushFrequency, softMemoryFlush: SoftMemoryFlushPercent): CacheBuilder[Key, Value] =
    new CacheBuilder[Key, Value] {
      def apply(sg: Semigroup[Value]) = {
        BackroundCompactionCache(cacheSize, flushFrequency, softMemoryFlush)(sg)
      }
    }
  def apply[Key, Value](cacheSize: CacheSize,
                        flushFrequency: FlushFrequency,
                        softMemoryFlush: SoftMemoryFlushPercent)
                        (implicit sg: Semigroup[Value]): AsyncCache[Key, Value] = {
    cacheSize.size.map { _ =>
      new NonEmptyBackroundCompactionCache[Key, Value](cacheSize, flushFrequency, softMemoryFlush)(sg)
    }.getOrElse(new EmptyBackroundCompactionCache[Key, Value]()(sg))
  }
}

private[summingbird] trait WithFlushConditions[Key, Value] extends AsyncCache[Key, Value] {
  protected var lastDump:Long = System.currentTimeMillis
  protected def softMemoryFlush: SoftMemoryFlushPercent
  protected def flushFrequency: FlushFrequency

  protected def timedOut = (System.currentTimeMillis - lastDump) >= flushFrequency.get.inMilliseconds
  protected lazy val runtime  = Runtime.getRuntime

  protected def didFlush {lastDump = System.currentTimeMillis}

  protected def memoryWaterMark = {
    val used = ((runtime.totalMemory - runtime.freeMemory).toDouble * 100) / runtime.maxMemory
    used > softMemoryFlush.get
  }
  def tick: Future[Map[Key, Value]] = {
    if (timedOut || memoryWaterMark) {
          forceTick
      }
    else {
      Future.value(Map.empty)
    }
  }
}

private[summingbird] trait ParallelCleanup[Key, Value] extends AsyncCache[Key, Value] {
  protected def executor: ExecutorService
  protected lazy val futurePool = FuturePool(executor)

  override def cleanup = {
    Future {
        executor.shutdown
        executor.awaitTermination(10, TimeUnit.SECONDS)
    }.flatMap(f => super.cleanup)
  }
}

class EmptyBackroundCompactionCache[Key, Value](implicit semigroup: Semigroup[Value])
                                          extends AsyncCache[Key, Value] {
  def forceTick: Future[Map[Key, Value]] = Future.value(Map.empty)
  def tick: Future[Map[Key, Value]] = Future.value(Map.empty)
  def insert(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = Future.value(Semigroup.sumOption(vals.map(Map(_))).getOrElse(Map.empty))
}


class NonEmptyBackroundCompactionCache[Key, Value](cacheSizeOpt: CacheSize,
                                          override val flushFrequency: FlushFrequency,
                                          override val softMemoryFlush: SoftMemoryFlushPercent)
                                         (implicit semigroup: Semigroup[Value])
                                          extends AsyncCache[Key, Value] with ParallelCleanup[Key, Value] with WithFlushConditions[Key, Value] {

  protected override val executor = Executors.newFixedThreadPool(2)
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  protected val cacheSize = cacheSizeOpt.size.get

  private val queue: ArrayBlockingQueue[Map[Key, Value]] = new ArrayBlockingQueue[Map[Key, Value]](cacheSizeOpt.size.get, true)

  override def forceTick: Future[Map[Key, Value]] = {
    val toSum = ListBuffer[Map[Key, Value]]()
    queue.drainTo(toSum.asJava)
    futurePool {
      Semigroup.sumOption(toSum).getOrElse(Map.empty)
    }
  }

  def insert(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] = {
    val curData = Semigroup.sumOption(vals.map(Map(_))).getOrElse(Map.empty)
    if(!queue.offer(curData)) {
      forceTick.map { flushRes =>
        Semigroup.plus(flushRes, curData)
      }
    }
    else {
      Future.value(Map.empty)
    }
  }
}
