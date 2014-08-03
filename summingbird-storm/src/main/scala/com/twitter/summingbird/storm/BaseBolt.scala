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

import scala.util.{ Try, Success, Failure }

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.IRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Tuple, TupleImpl, Fields }

import java.util.{ Map => JMap }
import com.twitter.summingbird.storm.option.{ AckOnEntry, AnchorTuples }
import com.twitter.summingbird.online.executor.OperationContainer
import com.twitter.summingbird.online.executor.{ InflightTuples, InputState }
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.{ JobCounters, SummingbirdRuntimeStats }

import scala.collection.JavaConverters._

import java.util.{ List => JList }
import org.slf4j.{ LoggerFactory, Logger }

/**
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */
case class BaseBolt[I, O](jobID: JobId,
    metrics: () => TraversableOnce[StormMetric[_]],
    anchorTuples: AnchorTuples,
    hasDependants: Boolean,
    outputFields: Fields,
    ackOnEntry: AckOnEntry,
    executor: OperationContainer[I, O, InputState[Tuple], JList[AnyRef], TopologyContext]) extends IRichBolt {

  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  val countersForBolt: List[(String, String)] =
    Try { JobCounters.registeredCountersForJob.get(jobID).toList }.toOption.getOrElse(Nil)

  private var collector: OutputCollector = null

  // Should we ack immediately on reception instead of at the end
  private val earlyAck = ackOnEntry.get

  protected def logError(message: String, err: Throwable) {
    collector.reportError(err)
    logger.error(message, err)
  }

  private def fail(inputs: List[InputState[Tuple]], error: Throwable): Unit = {
    executor.notifyFailure(inputs, error)
    if (!earlyAck) { inputs.foreach(_.fail(collector.fail(_))) }
    logError("Storm DAG of: %d tuples failed".format(inputs.size), error)
  }

  override def execute(tuple: Tuple) = {
    /**
     * System ticks come with a fixed stream id
     */
    val curResults = if (!tuple.getSourceStreamId.equals("__tick")) {
      val tsIn = executor.decoder.invert(tuple.getValues).get // Failing to decode here is an ERROR
      // Don't hold on to the input values
      clearValues(tuple)
      if (earlyAck) { collector.ack(tuple) }
      executor.execute(InputState(tuple), tsIn)
    } else {
      collector.ack(tuple)
      executor.executeTick
    }

    curResults.foreach {
      case (tups, res) =>
        res match {
          case Success(outs) => finish(tups, outs)
          case Failure(t) => fail(tups, t)
        }
    }
  }

  private def finish(inputs: List[InputState[Tuple]], results: TraversableOnce[O]) {
    var emitCount = 0
    if (hasDependants) {
      if (anchorTuples.anchor) {
        results.foreach { result =>
          collector.emit(inputs.map(_.state).asJava, executor.encoder(result))
          emitCount += 1
        }
      } else { // don't anchor
        results.foreach { result =>
          collector.emit(executor.encoder(result))
          emitCount += 1
        }
      }
    }
    // Always ack a tuple on completion:
    if (!earlyAck) { inputs.foreach(_.ack(collector.ack(_))) }

    logger.debug("bolt finished processed {} linked tuples, emitted: {}", inputs.size, emitCount)
  }

  override def prepare(conf: JMap[_, _], context: TopologyContext, oc: OutputCollector) {
    collector = oc
    metrics().foreach { _.register(context) }
    executor.init(context)
    StormStatProvider.registerMetrics(jobID, context, countersForBolt)
    SummingbirdRuntimeStats.addPlatformStatProvider(StormStatProvider)
    logger.debug("In Bolt prepare: added jobID stat provider for jobID {}", jobID)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    if (hasDependants) { declarer.declare(outputFields) }
  }

  override val getComponentConfiguration = null

  override def cleanup {
    executor.cleanup
  }

  /**
   * This is clearly not safe, but done to deal with GC issues since
   * storm keeps references to values
   */
  private lazy val valuesField: Option[(Tuple) => Unit] = {
    val tupleClass = classOf[TupleImpl]
    try {
      val vf = tupleClass.getDeclaredField("values")
      vf.setAccessible(true)
      Some({ t: Tuple => vf.set(t, null) })
    } catch {
      case _: NoSuchFieldException =>
        try {
          val m = tupleClass.getDeclaredMethod("resetValues", null)
          Some({ t: Tuple => m.invoke(t) })
        } catch {
          case _: NoSuchMethodException =>
            None
        }
    }
  }

  private def clearValues(t: Tuple): Unit = {
    valuesField.foreach(fn => fn(t))
  }
}
