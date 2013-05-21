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

package com.twitter.summingbird.storm

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.IRichBolt
import java.util.{ Map => JMap }

/**
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

abstract class BaseBolt[Key, Value](
  metrics: () => TraversableOnce[StormMetric[_]]) extends IRichBolt {
  class Mutex extends java.io.Serializable

  private var collector: OutputCollector = null

  val mutex = new Mutex
  def onCollector[U](fn: OutputCollector => U): U = mutex.synchronized { fn(collector) }

  override def prepare(conf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    // There is no need for a mutex here because this called once on start
    collector = oc
    metrics().foreach { _.register(context) }
  }

  override val getComponentConfiguration = null
}
