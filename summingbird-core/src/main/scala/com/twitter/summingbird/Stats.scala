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

import scala.collection.mutable.{ HashMap => MMap, Set => MSet }
import scala.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap


/**
 * @author Katya Gonina 
 */

trait PlatformMetricProvider {
  def incrementor(name: String): Long => Unit
}

object RuntimeStats {

  private val platformMetricProviderForJob: ConcurrentHashMap[String, WeakReference[PlatformMetricProvider]] =
    new ConcurrentHashMap[String, WeakReference[PlatformMetricProvider]]()

  // TODO: make sure this is thread-safe
  val registeredMetricsForJob = MMap[String, MSet[String]]()

  private def getPlaformMetricProviderForJobId(uniqueId: String): PlatformMetricProvider = 
    Option(platformMetricProviderForJob.get(uniqueId))
      .flatMap(_.get)
      .getOrElse {
        sys.error("Error in job deployment, the Platform Provider for unique id %s isn't available".format(uniqueId))
      }
  

  def addPlatformMetricProvider(jobID: String, pp: PlatformMetricProvider) {
    platformMetricProviderForJob.put(jobID, new WeakReference(pp))
  }

  def registerMetric(jobID: String, name: String) {
    if (platformMetricProviderForJob.size == 0) {
      val buf = registeredMetricsForJob.getOrElseUpdate(jobID, MSet[String]())
      buf += name
      println("ON SUBMITTER: Registered metric " + name + " for jobID " + jobID)
    }
  }

  def getPlatformMetric(jobID: String, name: String) =
    getPlaformMetricProviderForJobId(jobID).incrementor(name)
}

case class Stat(name: String)(implicit jobID: String) {
  RuntimeStats.registerMetric(jobID, name)

  lazy val metric: (Long => Unit) = RuntimeStats.getPlatformMetric(jobID, name)

  def incrBy(amount: Long) = metric(amount)

  def incr = incrBy(1L)
}