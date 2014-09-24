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

package com.twitter.summingbird.memory

import com.twitter.summingbird._
import com.twitter.summingbird.option.JobId
import java.util.concurrent.ConcurrentHashMap

/**
 * Mutable counter for the Memory platform
 */
class MemoryCounter {

  private var count: Long = 0L

  /**
   * Increment the counter
   * @param by - the amount to increment the counter by
   */
  def incr(by: Long): Unit = {
    count += by
  }

  /**
   * Get the counter value
   */
  def get: Long = count
}

/**
 * Incrementor for Memory Counters
 * Returned to the Summingbird Counter object to call incrBy function in job code
 */
private[summingbird] case class MemoryCounterIncrementor(counter: MemoryCounter) extends CounterIncrementor {
  def incrBy(by: Long): Unit = counter.incr(by)
}

/**
 * MemoryStatProvider object, contains counters for the Memory job
 */
private[summingbird] object MemoryStatProvider extends PlatformStatProvider {

  private val countersForJob = new ConcurrentHashMap[JobId, Map[String, MemoryCounter]]()

  /**
   * Returns the counters for the job
   */
  def getCountersForJob(jobID: JobId): Option[Map[String, MemoryCounter]] = {
    if (countersForJob.containsKey(jobID)) {
      Some(countersForJob.get(jobID))
    } else {
      None
    }
  }

  /**
   * Memory counter incrementor, used by the Counter object in Summingbird job
   */
  def counterIncrementor(passedJobId: JobId, group: String, name: String): Option[MemoryCounterIncrementor] = {
    if (countersForJob.containsKey(passedJobId)) {
      val counter = countersForJob.get(passedJobId).getOrElse(group + "/" + name,
        sys.error("It is only valid to create counter objects during job submission"))
      Some(MemoryCounterIncrementor(counter))
    } else {
      None
    }
  }

  /**
   * Registers counters with the MemoryStatProvider
   * @param jobID for the job
   * @param counters - list of counter names with format (group, name)
   */
  def registerCounters(jobID: JobId, counters: List[(String, String)]): Unit = {
    if (!countersForJob.containsKey(jobID)) {
      val memoryCounters = counters.map {
        case (group, name) =>
          (group + "/" + name, new MemoryCounter())
      }.toMap
      countersForJob.put(jobID, memoryCounters)
    }
  }
}