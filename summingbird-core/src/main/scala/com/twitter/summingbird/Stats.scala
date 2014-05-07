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

import scala.collection.mutable.{ SynchronizedSet, HashSet => MSet, HashMap => MMap }
import scala.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap


/**
 * @author Katya Gonina 
 */

trait PlatformMetricProvider {
  def incrementor(jobId: String, name: String): Option[Long => Unit]
}

object SBRuntimeStats {
  // TODO: use synchronized set
  val platformMetricProviders: MSet[WeakReference[PlatformMetricProvider]] =
    new MSet[WeakReference[PlatformMetricProvider]]()

  def addPlatformMetricProvider(pp: PlatformMetricProvider) {
    platformMetricProviders += new WeakReference(pp)
  }

  def getPlatformMetric(jobID: String, name: String) =
    (for {
      provRef <- platformMetricProviders
      prov <- provRef.get
      incr <- prov.incrementor(jobID, name)
      } yield incr)
      .headOption
      .getOrElse(sys.error("Could not find the platform metric provider, list of providers: " + platformMetricProviders mkString))
}

object JobMetrics{
  val registeredMetricsForJob = MMap[String, MSet[String]]()

  def registerMetric(jobID: String, name: String) {
    if (SBRuntimeStats.platformMetricProviders.size == 0) {
      val buf = registeredMetricsForJob.getOrElseUpdate(jobID, MSet[String]())
      buf += name
    }
  }
}

case class Stat(name: String)(implicit jobID: String) {
  JobMetrics.registerMetric(jobID, name)

  lazy val metric: (Long => Unit) = SBRuntimeStats.getPlatformMetric(jobID, name)

  def incrBy(amount: Long) = metric(amount)

  def incr = incrBy(1L)
}