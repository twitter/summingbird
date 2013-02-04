package com.twitter.summingbird

import com.twitter.scalding.Args

import com.twitter.summingbird.storm.StormEnv
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.builder.CompletedBuilder

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// Main app class launching any summingbird job. storm.StormEnv
// handles storm-specific job-launching, scalding.ScaldingEnv handles
// Scalding.

object Env {
  def apply(inargs : Array[String]) : Env = {
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

  def main(inargs : Array[String]) {
    Env(inargs).run
  }
}

/**
 * Any environment data an AbstractJob may need. This state is mutable.
 */
abstract class Env(val jobName : String) extends java.io.Serializable {
  val args : Args
  var builder : CompletedBuilder[_,_,_,_] = null

  // This is where the builder is actually populated.
  protected def abstractJob: AbstractJob = AbstractJob(jobName, this)
  def run : Unit
}
