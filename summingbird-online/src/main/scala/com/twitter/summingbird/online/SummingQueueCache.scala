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

import com.twitter.algebird.{ Semigroup, Monoid, SummingQueue }
import com.twitter.util.{Return, Throw}
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.online.option.FlushFrequency
import java.util.concurrent._
import scala.collection.JavaConversions._
import com.twitter.util.Future
import scala.collection.mutable.{Set => MSet, Queue => MQueue, Map => MMap}

import org.slf4j.{LoggerFactory, Logger}

/**
 * @author Ian O Connell
 */
object SummingQueueCache {
  def builder[Key, Value](cacheSize: CacheSize, flushFrequency: FlushFrequency) =
      {(sg: Semigroup[Value]) =>
            new SummingQueueCache[Key, Value](cacheSize, flushFrequency)(sg) }
}

case class SummingQueueCache[Key, Value](cacheSizeOpt: CacheSize, flushFrequency: FlushFrequency)
  (implicit semigroup: Semigroup[Value]) extends AsyncCache[Key, Value] {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val cacheSize = cacheSizeOpt.size.getOrElse(0)
  private val squeue: SummingQueue[Map[Key, Value]] = SummingQueue(cacheSize)
  @volatile private var lastDump:Long = System.currentTimeMillis

  private def timedOut = (System.currentTimeMillis - lastDump) > flushFrequency.get.inMilliseconds

  def forceTick: Future[Map[Key, Value]] = Future.value(squeue.flush.getOrElse(Map.empty))

  def tick: Future[Map[Key, Value]] =
    if(timedOut) {
      lastDump = System.currentTimeMillis
      forceTick
    }
    else {
        Future.value(Map.empty)
    }

  def insert(vals: TraversableOnce[(Key, Value)]): Future[Map[Key, Value]] =
    Future.value(Monoid.sum(vals.map(Map(_)).map(squeue.put(_).getOrElse(Map.empty))))
}
