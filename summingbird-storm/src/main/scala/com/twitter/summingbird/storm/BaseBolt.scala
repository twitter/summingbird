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
import backtype.storm.tuple.Tuple
import java.util.{ Map => JMap, Arrays => JArrays, List => JList }

import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.storm.option.{AnchorTuples, MaxWaitingFutures}

import scala.collection.JavaConverters._

import org.slf4j.{LoggerFactory, Logger}

/**
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */
abstract class BaseBolt[I,O](metrics: () => TraversableOnce[StormMetric[_]],
  anchorTuples: AnchorTuples,
  hasDependants: Boolean
  ) extends IRichBolt {

  def decoder: StormTupleInjection[I]
  def encoder: StormTupleInjection[O]

  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  private var collector: OutputCollector = null

  /**
   * IMPORTANT: only call this inside of an execute method.
   * storm is not safe to call methods on the emitter from other
   * threads.
   */
  protected def fail(inputs: JList[Tuple], error: Throwable): Unit = {
    inputs.iterator.asScala.foreach(collector.fail(_))
    collector.reportError(error)
    logger.error("Storm DAG of: %d tuples failed".format(inputs.size), error)
  }

  /**
   * IMPORTANT: only call this inside of an execute method.
   * storm is not safe to call methods on the emitter from other
   * threads.
   */
  protected def finish(inputs: JList[Tuple], results: TraversableOnce[(Timestamp, O)]) {
    var emitCount = 0
    if(hasDependants) {
      if(anchorTuples.anchor) {
        results.foreach { result =>
          collector.emit(inputs, encoder(result))
          emitCount += 1
        }
      }
      else { // don't anchor
        results.foreach { result =>
          collector.emit(encoder(result))
          emitCount += 1
        }
      }
    }
    // Always ack a tuple on completion:
    inputs.iterator.asScala.foreach(collector.ack(_))
    logger.debug("bolt finished processed %d linked tuples, emitted: %d"
      .format(inputs.size, emitCount))
  }

  override def prepare(conf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    collector = oc
    metrics().foreach { _.register(context) }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    if(hasDependants) { declarer.declare(encoder.fields) }
  }

  override val getComponentConfiguration = null

  override def cleanup { }
}
