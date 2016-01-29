/*
 Copyright 2014 Twitter, Inc.

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

package com.twitter.summingbird

import com.twitter.summingbird.option.JobId
import scala.collection.JavaConverters._
import scala.collection.parallel.mutable.ParHashSet
import scala.ref.WeakReference
import scala.util.Try
import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

trait CounterIncrementor {
  def incrBy(by: Long): Unit
}

trait PlatformStatProvider {
  // Incrementor for a Counter identified by group/name for the specific jobID
  // Returns an incrementor function for the Counter wrapped in an Option
  // to ensure we catch when the incrementor cannot be obtained for the specified jobID
  def counterIncrementor(jobId: JobId, group: Group, name: Name): Option[CounterIncrementor]
}

object SummingbirdRuntimeStats {
  private class MutableSetSynchronizedWrapper[T] {
    private[this] val innerContainer = scala.collection.mutable.Set[T]()
    def nonEmpty: Boolean = innerContainer.synchronized { innerContainer.nonEmpty }
    def toSeq: Seq[T] = innerContainer.synchronized { innerContainer.toSeq }
    def add(e: T): Unit = innerContainer.synchronized { innerContainer += e }
  }

  // A global set of PlatformStatProviders, ParHashSet in scala seemed to trigger a deadlock
  // So a simple wrapper on a mutable set is used.
  private[this] val platformStatProviders = new MutableSetSynchronizedWrapper[WeakReference[PlatformStatProvider]]

  val SCALDING_STATS_MODULE = "com.twitter.summingbird.scalding.ScaldingRuntimeStatsProvider$"

  // Need to explicitly invoke the object initializer on remote node
  // since Scala object initialization is lazy, hence need the absolute object classpath
  private[this] final val platformObjects = List(SCALDING_STATS_MODULE)

  // invoke the ScaldingRuntimeStatsProvider object initializer on remote node
  private[this] lazy val platformsInit =
    platformObjects.foreach { s: String => Try[Unit] { Class.forName(s) } }

  def hasStatProviders: Boolean = platformStatProviders.nonEmpty

  def addPlatformStatProvider(pp: PlatformStatProvider): Unit =
    platformStatProviders.add(new WeakReference(pp))

  def getPlatformCounterIncrementor(jobID: JobId, group: Group, name: Name): CounterIncrementor = {
    platformsInit
    // Find the PlatformMetricProvider (PMP) that matches the jobID
    // return the incrementor for the Counter specified by group/name
    // We return the first PMP that matches the jobID, in reality there should be only one
    (for {
      provRef <- platformStatProviders.toSeq
      prov <- provRef.get
      incr <- prov.counterIncrementor(jobID, group, name)
    } yield incr)
      .toList
      .headOption
      .getOrElse(sys.error("Could not find the platform stat provider for jobID " + jobID))
  }
}

object JobCounters {
  @annotation.tailrec
  private[this] final def getOrElseUpdate[K, V](map: ConcurrentHashMap[K, V], k: K, default: => V): V = {
    val v = map.get(k)
    if (v == null) {
      map.putIfAbsent(k, default)
      getOrElseUpdate(map, k, default)
    } else {
      v
    }
  }

  private val registeredCountersForJob: ConcurrentHashMap[JobId, ParHashSet[(Group, Name)]] =
    new ConcurrentHashMap[JobId, ParHashSet[(Group, Name)]]()

  def getCountersForJob(jobID: JobId): Option[Seq[(Group, Name)]] =
    Option(registeredCountersForJob.get(jobID)).map(_.toList)

  def registerCounter(jobID: JobId, group: Group, name: Name): Unit = {
    val set = getOrElseUpdate(registeredCountersForJob, jobID, ParHashSet[(Group, Name)]())
    set += ((group, name))
  }
}
