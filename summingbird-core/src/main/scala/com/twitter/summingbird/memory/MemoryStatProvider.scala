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
import java.util.concurrent.atomic.AtomicLong

/**
 * Mutable counter for the Memory platform
 */
class MemoryCounter {

  private val count: AtomicLong = new AtomicLong()

  /**
   * Increment the counter
   * @param by - the amount to increment the counter by
   */
  def incr(by: Long): Unit = {
    val c = count.addAndGet(by)
  }

  /**
   * Get the counter value
   */
  def get: Long = count.get()
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
  def getCountersForJob(jobID: JobId): Option[Map[String, MemoryCounter]] =
    Option(countersForJob.get(jobID))

  /**
   * Memory counter incrementor, used by the Counter object in Summingbird job
   */
  def counterIncrementor(jobID: JobId, group: Group, name: Name): Option[MemoryCounterIncrementor] =
    Option(countersForJob.get(jobID)).map { m =>
      MemoryCounterIncrementor(m.getOrElse(group.getString + "/" + name.getString,
        sys.error("It is only valid to create counter objects during job submission")))
    }

  /**
   * Registers counters with the MemoryStatProvider
   * @param jobID for the job
   * @param counters - list of counter names with format (group, name)
   */
  def registerCounters(jobID: JobId, counters: Seq[(Group, Name)]): Unit = {
    val memoryCounters = counters.map {
      case (group, name) =>
        (group.getString + "/" + name.getString, new MemoryCounter())
    }.toMap

    @annotation.tailrec
    def put(m: Map[String, MemoryCounter]): Unit =
      countersForJob.putIfAbsent(jobID, memoryCounters) match {
        case null => () // The jobID was not present
        case previous if (previous.keySet & m.keySet).nonEmpty => // Key intersection nonempty
          // prefer the old values
          if (countersForJob.replace(jobID, previous, (m ++ previous))) () else put(m)
        case _ => () // there is something there, but the keys are all present
      }

    put(memoryCounters)
  }
}