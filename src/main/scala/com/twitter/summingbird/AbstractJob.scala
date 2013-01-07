package com.twitter.summingbird

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Base class for all Summingbird jobs; all summingbird jobs
 * should extend AbstractJob.
 */

object AbstractJob {
  def apply(jobName: String, env: Env): AbstractJob =
    Class.forName(jobName)
      .getConstructor(classOf[Env])
      .newInstance(env)
      .asInstanceOf[AbstractJob]
}

// Subclass this to write your job.

abstract class AbstractJob(env: Env) extends java.io.Serializable {
  // Make these implicitly available. Lazy because the env doesn't
  // initially contain a reference to the builder, and the args
  // constructor might need to make use of it
  implicit lazy val _env = env
  implicit lazy val _args = env.args
  def transformConfig(m: Map[String,AnyRef]): Map[String,AnyRef] = m
}
