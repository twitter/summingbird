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

trait PlatformMetricProvider {
  // Incrementor for a Stat identified by group/name for the specific jobID
  // Returns an incrementor [Long=>Unit] function for the Stat wrapped in an Option
  // to ensure we catch when the incrementor cannot be obtained for the specified jobID
  def incrementor(jobId: SummingbirdJobID, group: String, name: String): Option[Long => Unit]
}

case class SummingbirdJobID(get: String)

object SBRuntimeStats {
  // Need to explicitly invoke the object initializer on remote node
  // since Scala object initialization is lazy, hence need the absolute object classpath
  private[this] final val platformObjects = List("com.twitter.summingbird.scalding.ScaldingRuntimeStatsProvider")

  // invoke the ScaldingRuntimeStatsProvider object initializer on remote node
  private[this] lazy val platformsInit =
    platformObjects.foreach { s: String => ScalaTry[Unit]{ Class.forName(s + "$") } }

  // A global set of PlatformMetricProviders, use Java ConcurrentHashMap to create a thread-safe set
  val platformMetricProviders: MSet[WeakReference[PlatformMetricProvider]] =
    Collections.newSetFromMap[WeakReference[PlatformMetricProvider]](
      new ConcurrentHashMap[WeakReference[PlatformMetricProvider], java.lang.Boolean]())
      .asScala

  def addPlatformMetricProvider(pp: PlatformMetricProvider) =
    platformMetricProviders += new WeakReference(pp)

  def getPlatformMetricIncrementor(jobID: SummingbirdJobID, group: String, name: String): Long => Unit = {
    platformsInit
    // Find the PlatformMetricProvider that matches the jobID
    // return the incrementor for the Stat specified by group/name
    // We return 
    (for {
      provRef <- platformMetricProviders
      prov <- provRef.get
      incr <- prov.incrementor(jobID, group, name)
      } yield incr)
      .headOption
      .getOrElse(sys.error("Could not find the platform metric provider for jobID " + jobID))
    }
}

object JobMetrics{
  val registeredMetricsForJob = MMap[SummingbirdJobID, MHashSet[(String, String)]]()

  def registerMetric(jobID: SummingbirdJobID, group: String, name: String) {
    if (SBRuntimeStats.platformMetricProviders.isEmpty) {
      val set = registeredMetricsForJob.getOrElseUpdate(jobID, MHashSet[(String, String)]())
      set += ((group, name))
    }
  }
}

case class Stat(group: String, name: String)(implicit jobID: SummingbirdJobID) {
  // Need to register the metric for this job, passed to Storm platform during initialization to register metrics
  JobMetrics.registerMetric(jobID, group, name)

  private lazy val incrMetric: (Long => Unit) = SBRuntimeStats.getPlatformMetricIncrementor(jobID, group, name)

  def incrBy(amount: Long) = incrMetric(amount)

  def incr = incrBy(1L)
}
