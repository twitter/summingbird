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

case class Group(getString: String) extends AnyVal

case class Name(getString: String) extends AnyVal

/*
 User-defined Counter
*/
case class Counter(group: Group, name: Name)(implicit jobID: JobId) {
  // Need to register the counter for this job,
  // this is used to pass counter info to the Storm platform during initialization
  JobCounters.registerCounter(jobID, group, name)

  private[this] final lazy val incrCounter: CounterIncrementor =
    SummingbirdRuntimeStats.getPlatformCounterIncrementor(jobID, group, name)

  def incrBy(amount: Long): Unit = incrCounter.incrBy(amount)

  def incr(): Unit = incrBy(1L)
}
