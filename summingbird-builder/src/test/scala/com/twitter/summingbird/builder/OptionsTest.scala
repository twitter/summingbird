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

package com.twitter.summingbird.builder

import com.twitter.summingbird.scalding.{Scalding, ConfigBijection, ScaldingEnv, InitialBatchedStore}
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{Batcher, BatchID}
import com.twitter.summingbird.option._
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.store.CompoundStore
import org.specs2.mutable._

import org.apache.hadoop.conf.Configuration

import com.twitter.bijection.Conversion.asMethod
import ConfigBijection._

class TestJob1(env: Env) extends AbstractJob(env) {

  implicit def batcher = Batcher.ofHours(1)

  try {
  EventSource[Long](Some(null), None)
    .withTime(new java.util.Date(_))
    .map { e => (e % 2, e) }
    .groupAndSumTo(CompoundStore.fromOffline[Long, Long](new InitialBatchedStore(BatchID(12L), null)))
    .set(MonoidIsCommutative(true))
  }
  catch {
    case t: Throwable => t.printStackTrace
  }
}

class OptionsTest extends Specification {
  "Commutative Options should not be lost" in {
    val scalding = ScaldingEnv("com.twitter.summingbird.builder.TestJob1",
      Array("-Dcascading.aggregateby.threshold=100000", "--test", "arg"))

    scalding.build.platform.jobName must be_==("com.twitter.summingbird.builder.TestJob1")

    val conf = new Configuration
    scalding.build.platform.updateConfig(conf)
    conf.get("com.twitter.chill.config.configuredinstantiator") must not beNull;
    conf.get("summingbird.options") must be_==(scalding.build.platform.options.toString)
    conf.get("cascading.aggregateby.threshold") must be_==("100000")

    val opts = scalding.build.platform.options
    val dependants = Dependants(scalding.build.toRun)
    val summers = dependants.nodes.collect { case s: Summer[_, _, _] => s }

    summers.size must be_==(1)
    val names = dependants.namesOf(summers.head).map(_.id)
    Scalding
      .getCommutativity(names,
        opts,
        summers.head.asInstanceOf[Summer[Scalding,_,_]]) must be_==(Commutative)
  }
}
