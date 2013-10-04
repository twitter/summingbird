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
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple }
import java.util.{ Map => JMap }

/**
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

abstract class BaseBolt(metrics: () => TraversableOnce[StormMetric[_]]) extends IRichBolt {
  class Mutex extends java.io.Serializable

  /**
    * The fields this bolt plans on returning.
    */
  def fields: Option[Fields]

  private var collector: OutputCollector = null

  val mutex = new Mutex
  def onCollector[U](fn: OutputCollector => U): U =
    mutex.synchronized { fn(collector) }

  def ack(tuple: Tuple) { onCollector { _.ack(tuple) } }

  override def prepare(
    conf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    /**
      * No need for a mutex here because this called once on
      * start
      */
    collector = oc
    metrics().foreach { _.register(context) }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    fields.foreach(declarer.declare(_))
  }

  override val getComponentConfiguration = null

  override def cleanup { }
}
