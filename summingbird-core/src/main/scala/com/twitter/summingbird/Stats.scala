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

import scala.collection.mutable.{ Set => MSet, HashSet => MHashSet, HashMap => MMap }
import scala.collection.JavaConverters._
import scala.ref.WeakReference
import scala.util.{ Try => ScalaTry }
import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

trait CounterIncrementor {
  def incrBy(by: Long): Unit
}

trait PlatformStatProvider {
  // Incrementor for a Counter identified by group/name for the specific jobID
  // Returns an incrementor [Long=>Unit] function for the Counter wrapped in an Option
  // to ensure we catch when the incrementor cannot be obtained for the specified jobID
  def counterIncrementor(jobId: SummingbirdJobId, group: String, name: String): Option[CounterIncrementor]
}

case class SummingbirdJobId(get: String)

object SummingbirdRuntimeStats {
  val SCALDING_STATS_MODULE = "com.twitter.summingbird.scalding.ScaldingRuntimeStatsProvider$"

  // Need to explicitly invoke the object initializer on remote node
  // since Scala object initialization is lazy, hence need the absolute object classpath
  private[this] final val platformObjects = List(SCALDING_STATS_MODULE)

  // invoke the ScaldingRuntimeStatsProvider object initializer on remote node
  private[this] lazy val platformsInit =
    platformObjects.foreach { s: String => ScalaTry[Unit]{ Class.forName(s) } }

  // A global set of PlatformStatProviders, use Java ConcurrentHashMap to create a thread-safe set
  val platformStatProviders: MSet[WeakReference[PlatformStatProvider]] =
    Collections.newSetFromMap[WeakReference[PlatformStatProvider]](
      new ConcurrentHashMap[WeakReference[PlatformStatProvider], java.lang.Boolean]())
      .asScala

  def addPlatformStatProvider(pp: PlatformStatProvider) =
    platformStatProviders += new WeakReference(pp)

  def getPlatformCounterIncrementor(jobID: SummingbirdJobId, group: String, name: String): CounterIncrementor = {
    platformsInit
    // Find the PlatformMetricProvider (PMP) that matches the jobID
    // return the incrementor for the Counter specified by group/name
    // We return the first PMP that matches the jobID, in reality there should be only one
    (for {
      provRef <- platformStatProviders
      prov <- provRef.get
      incr <- prov.counterIncrementor(jobID, group, name)
      } yield incr)
      .headOption
      .getOrElse(sys.error("Could not find the platform stat provider for jobID " + jobID))
    }
}

object JobCounters{
  val registeredCountersForJob = MMap[SummingbirdJobId, MHashSet[(String, String)]]()

  def registerCounter(jobID: SummingbirdJobId, group: String, name: String) {
    if (SummingbirdRuntimeStats.platformStatProviders.isEmpty) {
      val set = registeredCountersForJob.getOrElseUpdate(jobID, MHashSet[(String, String)]())
      set += ((group, name))
    }
  }
}

case class Counter(group: String, name: String)(implicit jobID: SummingbirdJobId) {
  // Need to register the counter for this job,
  // this is used to pass counter info to the Storm platform during initialization
  JobCounters.registerCounter(jobID, group, name)

  private lazy val incrCounter: CounterIncrementor =
    SummingbirdRuntimeStats.getPlatformCounterIncrementor(jobID, group, name)

  def incrBy(amount: Long) = incrCounter.incrBy(amount)

  def incr = incrBy(1L)
}
