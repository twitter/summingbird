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

import scala.util.{ Success, Failure }

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.IRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Tuple, TupleImpl, Fields }

import java.util.{ Map => JMap }
import com.twitter.summingbird.storm.option.{ AckOnEntry, AnchorTuples, MaxExecutePerSecond }
import com.twitter.summingbird.online.executor.OperationContainer
import com.twitter.summingbird.online.executor.{ InflightTuples, InputState }
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.{ Group, JobCounters, Name, SummingbirdRuntimeStats }
import com.twitter.summingbird.online.Externalizer

import scala.collection.JavaConverters._

import java.util.{ List => JList }
import org.slf4j.{ LoggerFactory, Logger }

/**
 *
 * @author Oscar Boykin
 * @author Ian O Connell
 * @author Sam Ritchie
 * @author Ashu Singhal
 */
case class BaseBolt[I, O](jobID: JobId,
    metrics: () => TraversableOnce[StormMetric[_]],
    anchorTuples: AnchorTuples,
    hasDependants: Boolean,
    outputFields: Fields,
    ackOnEntry: AckOnEntry,
    maxExecutePerSec: MaxExecutePerSecond,
    executor: OperationContainer[I, O, InputState[Tuple], JList[AnyRef], TopologyContext]) extends IRichBolt {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[this] val lockedCounters = Externalizer(JobCounters.getCountersForJob(jobID).getOrElse(Nil))

  lazy val countersForBolt: Seq[(Group, Name)] = lockedCounters.get

  private var collector: OutputCollector = null

  private[this] var executedThisPeriod = 0L
  private[this] var lastPeriod = 0L

  private[this] val lowerBound = maxExecutePerSec.lowerBound * 10
  private[this] val upperBound = maxExecutePerSec.upperBound * 10
  private[this] val PERIOD_LENGTH_MS = 10000L

  private[this] val rampPeriods = maxExecutePerSec.rampUptimeMS / PERIOD_LENGTH_MS
  private[this] val deltaPerPeriod: Long = if (rampPeriods > 0) (upperBound - lowerBound) / rampPeriods else 0

  private[this] lazy val startPeriod = System.currentTimeMillis / PERIOD_LENGTH_MS
  private[this] lazy val endRampPeriod = startPeriod + rampPeriods

  /*
    This is the rate limit how many tuples we allow the bolt to process per second.
    Due to variablitiy in how many things arrive we expand it out and work in 10 second blocks.
    This does a linear ramp up from the lower bound to the upper bound over the time period.
  */
  @annotation.tailrec
  private def rateLimit(): Unit = {
    val sleepTime = this.synchronized {
      val baseTime = System.currentTimeMillis
      val currentPeriod = baseTime / PERIOD_LENGTH_MS
      val timeTillNextPeriod = (currentPeriod + 1) * PERIOD_LENGTH_MS - baseTime

      if (currentPeriod == lastPeriod) {
        val maxPerPeriod = if (currentPeriod < endRampPeriod) {
          ((currentPeriod - startPeriod) * deltaPerPeriod) + lowerBound
        } else {
          upperBound
        }
        if (executedThisPeriod > maxPerPeriod) {
          timeTillNextPeriod
        } else {
          executedThisPeriod = executedThisPeriod + 1
          0
        }
      } else {
        lastPeriod = currentPeriod
        executedThisPeriod = 0L
        0
      }
    }
    if (sleepTime > 0) {
      Thread.sleep(sleepTime)
      rateLimit()
    } else {
      ()
    }
  }

  // Should we ack immediately on reception instead of at the end
  private val earlyAck = ackOnEntry.get

  protected def logError(message: String, err: Throwable) {
    collector.reportError(err)
    logger.error(message, err)
  }

  private def fail(inputs: Seq[InputState[Tuple]], error: Throwable): Unit = {
    executor.notifyFailure(inputs, error)
    if (!earlyAck) { inputs.foreach(_.fail(collector.fail(_))) }
    logError("Storm DAG of: %d tuples failed".format(inputs.size), error)
  }

  override def execute(tuple: Tuple) = {
    rateLimit()
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

  private def finish(inputs: Seq[InputState[Tuple]], results: TraversableOnce[O]) {
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
