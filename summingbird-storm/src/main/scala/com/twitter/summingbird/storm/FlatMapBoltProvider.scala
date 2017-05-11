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

import Constants._
import com.twitter.algebird.Semigroup
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.storm.option.AnchorTuples
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner._
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.storm.planner._
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.slf4j.LoggerFactory
import scala.collection.{ Map => CMap }

/**
 * These are helper functions for building a bolt from a Node[Storm] element.
 * There are two main codepaths here, for intermediate flat maps and final flat maps.
 * The primary difference between those two being the the presents of map side aggreagtion in a final flatmap.
 */
object FlatMapBoltProvider {
  @transient private val logger = LoggerFactory.getLogger(FlatMapBoltProvider.getClass)
  private def wrapTimeBatchIDKV[T, K, V](existingOp: FlatMapOperation[T, (K, V)])(batcher: Batcher): FlatMapOperation[(Timestamp, T), ((K, BatchID), (Timestamp, V))] =
    FlatMapOperation.generic[(Timestamp, T), ((K, BatchID), (Timestamp, V))]({
      case (ts, data) =>
        existingOp.apply(data).map { vals =>
          vals.map {
            case (k, v) =>
              ((k, batcher.batchOf(ts)), (ts, v))
          }
        }
    })

  def wrapTime[T, U](existingOp: FlatMapOperation[T, U]): FlatMapOperation[(Timestamp, T), (Timestamp, U)] = {
    FlatMapOperation.generic({ x: (Timestamp, T) =>
      existingOp.apply(x._2).map { vals =>
        vals.map((x._1, _))
      }
    })
  }
}

case class FlatMapBoltProvider(storm: Storm, jobID: JobId, stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) {
  import FlatMapBoltProvider._
  import Producer2FlatMapOperation._

  def getOrElse[T <: AnyRef: Manifest](default: T, queryNode: StormNode = node) = storm.getOrElse(stormDag, queryNode, default)

  // Boilerplate extracting of the options from the DAG
  private val nodeName = stormDag.getNodeName(node)
  private val metrics = getOrElse(DEFAULT_FM_STORM_METRICS)
  private val anchorTuples = getOrElse(AnchorTuples.default)
  logger.info(s"[$nodeName] Anchoring: ${anchorTuples.anchor}")

  private val maxWaiting = getOrElse(DEFAULT_MAX_WAITING_FUTURES)
  private val maxWaitTime = getOrElse(DEFAULT_MAX_FUTURE_WAIT_TIME)
  logger.info(s"[$nodeName] maxWaiting: ${maxWaiting.get}")

  private val ackOnEntry = getOrElse(DEFAULT_ACK_ON_ENTRY)
  logger.info(s"[$nodeName] ackOnEntry : ${ackOnEntry.get}")

  val maxExecutePerSec = getOrElse(DEFAULT_MAX_EXECUTE_PER_SEC)
  logger.info(s"[$nodeName] maxExecutePerSec : $maxExecutePerSec")

  private val maxEmitPerExecute = getOrElse(DEFAULT_MAX_EMIT_PER_EXECUTE)
  logger.info(s"[$nodeName] maxEmitPerExecute : ${maxEmitPerExecute.get}")

  private def getFFMBolt[T, K, V](summer: SummerNode[Storm]) = {
    type ExecutorInput = (Timestamp, T)
    type ExecutorKey = Int
    type InnerValue = (Timestamp, V)
    type ExecutorValue = CMap[(K, BatchID), InnerValue]
    val summerProducer = summer.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Storm, K, V]]
    // When emitting tuples between the Final Flat Map and the summer we encode the timestamp in the value
    // The monoid we use in aggregation is timestamp max.
    val batcher = summerProducer.store.mergeableBatcher
    implicit val valueMonoid: Semigroup[V] = summerProducer.semigroup

    // Query to get the summer paralellism of the summer down stream of us we are emitting to
    // to ensure no edge case between what we might see for its parallelism and what it would see/pass to storm.
    val summerParalellism = getOrElse(DEFAULT_SUMMER_PARALLELISM, summer)
    val summerBatchMultiplier = getOrElse(DEFAULT_SUMMER_BATCH_MULTIPLIER, summer)

    // This option we report its value here, but its not user settable.
    val keyValueShards = executor.KeyValueShards(summerParalellism.parHint * summerBatchMultiplier.get)
    logger.info(s"[$nodeName] keyValueShards : ${keyValueShards.get}")

    val operation = foldOperations[T, (K, V)](node.members.reverse)
    val wrappedOperation = wrapTimeBatchIDKV(operation)(batcher)

    val builder = BuildSummer(storm, stormDag, node, jobID)

    BaseBolt(
      jobID,
      metrics.metrics,
      anchorTuples,
      true,
      new Fields(AGG_KEY, AGG_VALUE),
      ackOnEntry,
      maxExecutePerSec,
      new SingleItemInjection[ExecutorInput],
      new KeyValueInjection[ExecutorKey, ExecutorValue],
      new executor.FinalFlatMap(
        wrappedOperation,
        builder,
        maxWaiting,
        maxWaitTime,
        maxEmitPerExecute,
        keyValueShards
      )(implicitly[Semigroup[InnerValue]])
    )
  }

  def getIntermediateFMBolt[T, U] = {
    type ExecutorInput = (Timestamp, T)
    type ExecutorOutput = (Timestamp, U)

    val operation = foldOperations[T, U](node.members.reverse)
    val wrappedOperation = wrapTime(operation)

    BaseBolt(
      jobID,
      metrics.metrics,
      anchorTuples,
      stormDag.dependantsOf(node).size > 0,
      new Fields(VALUE_FIELD),
      ackOnEntry,
      maxExecutePerSec,
      new SingleItemInjection[ExecutorInput],
      new SingleItemInjection[ExecutorOutput],
      new executor.IntermediateFlatMap(
        wrappedOperation,
        maxWaiting,
        maxWaitTime,
        maxEmitPerExecute
      )
    )
  }

  def apply: BaseBolt[Any, Any] = {
    val summerOpt: Option[SummerNode[Storm]] = stormDag.dependantsOf(node).collect { case s: SummerNode[Storm] => s }.headOption
    summerOpt match {
      case Some(s) => getFFMBolt[Any, Any, Any](s).asInstanceOf[BaseBolt[Any, Any]]
      case None => getIntermediateFMBolt[Any, Any].asInstanceOf[BaseBolt[Any, Any]]
    }
  }
}
