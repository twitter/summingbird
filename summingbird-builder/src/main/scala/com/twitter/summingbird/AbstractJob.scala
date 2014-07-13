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

import com.twitter.chill.IKryoRegistrar

/**
 * Base class for all Summingbird jobs; all summingbird jobs
 * should extend AbstractJob.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
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
  def transformConfig(m: Map[String, AnyRef]): Map[String, AnyRef] = m
  def registrars: List[IKryoRegistrar] = Nil
}
