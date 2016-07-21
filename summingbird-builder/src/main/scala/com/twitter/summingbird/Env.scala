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

package com.twitter.summingbird

import com.twitter.scalding.Args

import com.twitter.summingbird.storm.StormEnv
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.builder.CompletedBuilder

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// Main app class launching any summingbird job. storm.StormEnv
// handles storm-specific job-launching, scalding.ScaldingEnv handles
// Scalding.

object Env {
  def apply(inargs: Array[String]): Env = {
    val mode = inargs(0)
    val job = inargs(1)
    val restArgs = inargs.tail.tail
    mode match {
      case "scalding" => {
        // Hadoop args must be processed specially:
        ScaldingEnv(job, restArgs)
      }
      case "storm" => {
        // Skip "storm" and the jobname:
        StormEnv(job, Args(restArgs))
      }
      case _ => sys.error("Unrecognized mode: " + mode + ", try: storm or scalding")
    }
  }

  def main(inargs: Array[String]) {
    Env(inargs).run()
  }
}

/**
 * Any environment data an AbstractJob may need. This state is mutable.
 */
abstract class Env(val jobName: String) extends java.io.Serializable {
  val args: Args
  @transient var builder: CompletedBuilder[_, _, _] = null

  // This is where the builder is actually populated.
  protected def abstractJob: AbstractJob = AbstractJob(jobName, this)
  def run(): Unit
}
