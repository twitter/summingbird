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

package com.twitter.summingbird.storm.builder

import chain.Chain
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.online.executor.{ InputState, OperationContainer }
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.storm.{ StormMetric, StormStatProvider }
import com.twitter.summingbird.storm.option.{ AckOnEntry, AnchorTuples, MaxExecutePerSecond }
import com.twitter.summingbird.{ Group, JobCounters, Name, SummingbirdRuntimeStats }
import java.util.{ Map => JMap }
import org.apache.storm.task.{ OutputCollector, TopologyContext }
import org.apache.storm.topology.{ IRichBolt, OutputFieldsDeclarer }
import org.apache.storm.tuple.{ Tuple, TupleImpl }
import org.slf4j.{ Logger, LoggerFactory }
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success }

/**
  * This class is used as an implementation for Storm's Bolts.
  * We avoid [[List]]'s as a parameters because they cause Scala serialization issues
  * see (https://stackoverflow.com/questions/40607954/scala-case-class-serialization).
  *
  * @param jobId of current topology, used for metrics.
  * @param boltId of current bolt, used for logging.
  * @param metrics we want to collect on this node.
  * @param anchorTuples should be equal to true if you want to utilize Storm's anchoring of tuples,
  *                     false otherwise.
  * @param ackOnEntry ack tuples in the beginning of processing.
  * @param maxExecutePerSec limits number of executes per second, will block processing thread after.
  *                         Used for rate limiting.
  * @param inputEdges vector of edges incoming to this bolt.
  * @param outputEdges vector of edges outgoing from this bolt.
  * @param executor [[OperationContainer]] which represents operation for this bolt,
  *                 for example it can be summing or flat mapping.
  * @tparam I type of input tuples for this Bolts executor.
  * @tparam O type of output tuples for this Bolts executor.
  */
private[builder] case class BaseBolt[I, O](
  jobId: JobId,
  boltId: Topology.BoltId[I, O],
  metrics: () => TraversableOnce[StormMetric[_]],
  anchorTuples: AnchorTuples,
  ackOnEntry: AckOnEntry,
  maxExecutePerSec: MaxExecutePerSecond,
  inputEdges: Vector[Topology.Edge[_, I]],
  outputEdges: Vector[Topology.Edge[O, _]],
  executor: OperationContainer[I, O, InputState[Tuple]]
) extends IRichBolt {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[this] val lockedCounters = Externalizer(JobCounters.getCountersForJob(jobId).getOrElse(Nil))

  lazy val countersForBolt: Seq[(Group, Name)] = lockedCounters.get

  private var collector: OutputCollector = null

  private[this] var executedThisPeriod = 0L
  private[this] var lastPeriod = 0L

  private[this] val lowerBound = maxExecutePerSec.lowerBound * 10
  private[this] val upperBound = maxExecutePerSec.upperBound * 10
  private[this] val PERIOD_LENGTH_MS = 10000L

  private[this] val rampPeriods = maxExecutePerSec.rampUptimeMS / PERIOD_LENGTH_MS
  private[this] val deltaPerPeriod: Long = if (rampPeriods > 0) (upperBound - lowerBound) / rampPeriods else 0

  private[this] val inputEdgesMap: Map[String, Topology.Edge[_, I]] =
    // We forbid more than one edge between each pair of components, therefore there is only one edge
    // per each `edge.source.id`.
    inputEdges.map(edge => (edge.source.id, edge)).toMap

  private[this] val outputFormat: Option[OutputFormat[O]] = OutputFormat.get(outputEdges)

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

  private def fail(inputs: Chain[InputState[Tuple]], error: Throwable): Unit = {
    executor.notifyFailure(inputs, error)
    if (!earlyAck) { inputs.foreach(_.fail(collector.fail(_))) }
    logError("Storm DAG of: %d tuples failed".format(inputs.iterator.size), error)
  }

  override def execute(tuple: Tuple) = {
    rateLimit()
    /**
     * System ticks come with a fixed stream id
     */
    val curResults = if (!tuple.getSourceStreamId.equals("__tick")) {
      val tsIn = inputEdgesMap.get(tuple.getSourceComponent) match {
        case Some(edge) => edge.deserialize(tuple.getValues).get
        case None => throw new Exception("Unrecognized source component: " +
          tuple.getSourceComponent+", current bolt: " + boltId.id)
      }
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

  private def finish(inputs: Chain[InputState[Tuple]], results: TraversableOnce[O]) {
    var emitCount = 0
    outputFormat.foreach { format =>
      if (anchorTuples.anchor) {
        val states = inputs.iterator.map(_.state).toList.asJava
        results.foreach { result =>
          collector.emit(states, format.injection(result))
          emitCount += 1
        }
      } else { // don't anchor
        results.foreach { result =>
          collector.emit(format.injection(result))
          emitCount += 1
        }
      }
    }
    // Always ack a tuple on completion:
    if (!earlyAck) { inputs.foreach(_.ack(collector.ack(_))) }

    if (logger.isDebugEnabled()) {
      logger.debug("bolt finished processed {} linked tuples, emitted: {}", inputs.iterator.size, emitCount)
    }
  }

  override def prepare(conf: JMap[_, _], context: TopologyContext, oc: OutputCollector) {
    collector = oc
    metrics().foreach { _.register(context) }
    executor.init()
    StormStatProvider.registerMetrics(jobId, context, countersForBolt)
    SummingbirdRuntimeStats.addPlatformStatProvider(StormStatProvider)
    logger.debug("In Bolt prepare: added jobId stat provider for jobID {}", jobId)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
   outputFormat.foreach(format => declarer.declare(format.asStormFields))
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
