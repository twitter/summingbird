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
import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

trait PlatformMetricProvider {
  def incrementor(jobId: String, group: String, name: String): Option[Long => Unit]
}

object SBRuntimeStats {
  // Need to explicitly invoke the object initializer on remote node
  // since Scala object initializaiton is lazy, hence need the absolute object classpath
  private[this] final val platformObjects = List("com.twitter.summingbird.scalding.ScaldingRuntimeStatsProvider")

  // invoke the ScaldingRuntimeStatsProvider object initializer on remote node
  private[this] lazy val platformsInit = {
    platformObjects.foreach { s: String => 
     try { Class.forName(s + "$") } 
    }
  }
  val platformMetricProviders: MSet[WeakReference[PlatformMetricProvider]] =
    Collections.newSetFromMap[WeakReference[PlatformMetricProvider]](
      new ConcurrentHashMap[WeakReference[PlatformMetricProvider], java.lang.Boolean]())
      .asScala

  def addPlatformMetricProvider(pp: PlatformMetricProvider) {
    platformMetricProviders += new WeakReference(pp)
  }

  def getPlatformMetricIncrementor(jobID: String, group: String, name: String): Long => Unit = {
    platformsInit
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
  val registeredMetricsForJob = MMap[String, MHashSet[(String, String)]]()

  def registerMetric(jobID: String, group: String, name: String) {
    if (SBRuntimeStats.platformMetricProviders.size == 0) {
      val set = registeredMetricsForJob.getOrElseUpdate(jobID, MHashSet[(String, String)]())
      set += ((group, name))
    }
  }
}

case class Stat(group: String, name: String)(implicit jobID: String) {
  JobMetrics.registerMetric(jobID, group, name)

  lazy val incrMetric: (Long => Unit) = SBRuntimeStats.getPlatformMetricIncrementor(jobID, group, name)

  def incrBy(amount: Long) = incrMetric(amount)

  def incr = incrBy(1L)
}
