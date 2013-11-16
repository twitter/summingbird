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
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap, Arrays => JArrays, List => JList, ArrayList => JAList }

import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.storm.option.{AnchorTuples, MaxWaitingFutures}

import com.twitter.summingbird.online.Channel

import com.twitter.util.{Await, Future, Return, Throw, Try}
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

  // Make sure this is lazy/serializable when you implement
  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  private var collector: OutputCollector = null

  protected def fail(inputs: JList[Tuple], error: Throwable): Unit = {
    inputs.iterator.asScala.foreach(collector.fail(_))
    collector.reportError(error)
    logger.error("Storm DAG of: %d tuples failed".format(inputs.size), error)
  }

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
    /**
      * No need for a mutex here because this called once on
      * start
      */
    collector = oc
    metrics().foreach { _.register(context) }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    if(hasDependants) { declarer.declare(encoder.fields) }
  }

  override val getComponentConfiguration = null

  override def cleanup { }
}

abstract class AsyncBaseBolt[I, O](metrics: () => TraversableOnce[StormMetric[_]],
  anchorTuples: AnchorTuples,
  maxWaitingFutures: MaxWaitingFutures,
  hasDependants: Boolean) extends BaseBolt[I, O](metrics, anchorTuples, hasDependants) {

  /** If you can use Future.value below, do so. The double Future is here to deal with
   * cases that need to complete operations after or before doing a FlatMapOperation or
   * doing a store merge
   */
  def apply(tup: Tuple, in: (Timestamp, I)): Future[Iterable[(JList[Tuple], Future[TraversableOnce[(Timestamp, O)]])]]

  private lazy val futureQueue = Channel[Future[Unit]]()
  private lazy val channel = Channel[(JList[Tuple], Try[TraversableOnce[(Timestamp, O)]])]()

  override def execute(tuple: Tuple) {
    /**
     * System ticks come with a fixed stream id
     */
    if(!tuple.getSourceStreamId.equals("__tick")) {
      // This not a tick tuple so we need to start an async operation
      val tsIn = decoder.invert(tuple.getValues).get // Failing to decode here is a ERROR

      val fut = apply(tuple, tsIn)
        .onSuccess { iter =>
          // Collect the result onto our channel
          val (puts, maxSize) = iter.foldLeft((0, 0)) { case ((p, ms), (tups, res)) =>
            res.respond { t => channel.put((tups, t)) }
            // Make sure there are not too many outstanding:
            val count = futureQueue.put(res.unit)
            (p + 1, ms max count)
          }

          if(maxSize > maxWaitingFutures.get) {
            /*
             * This can happen on large key expansion.
             * May indicate maxWaitingFutures is too low.
             */
            logger.debug(
              "Exceeded maxWaitingFutures(%d): waiting = %d, put = %d"
                .format(maxWaitingFutures.get, maxSize, puts)
              )
          }
        }
        .onFailure { thr => fail(JArrays.asList(tuple), thr) }

      futureQueue.put(fut.unit)
    }
    // always empty the channel, even on tick
    emptyChannel
  }

  protected def forceExtraFutures {
    val toForce = futureQueue.trimTo(maxWaitingFutures.get)
    if(!toForce.isEmpty) Await.result(Future.collect(toForce))
  }

  protected def emptyChannel = {
    // don't let too many futures build up
    forceExtraFutures
    // Handle all ready results now:
    channel.foreach { case (tups, res) =>
      res match {
        case Return(outs) => finish(tups, outs)
        case Throw(t) => fail(tups, t)
      }
    }
  }
}
