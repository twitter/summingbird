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
import com.twitter.summingbird.batch.{ Batcher, Timestamp }
import com.twitter.summingbird.storm.option.AnchorTuples
import com.twitter.summingbird.planner._
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.storm.StormTopologyBuilder._
import com.twitter.summingbird.storm.builder.Topology
import com.twitter.summingbird.storm.planner._
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

private[storm] object FlatMapBoltProvider {
  @transient private val logger = LoggerFactory.getLogger(FlatMapBoltProvider.getClass)
  private def wrapTimeBatchIDKV[T, K, V](existingOp: FlatMapOperation[T, (K, V)])(
    batcher: Batcher
  ): FlatMapOperation[Item[T], (AggregateKey[K], AggregateValue[V])] =
    FlatMapOperation.generic[Item[T], (AggregateKey[K], AggregateValue[V])]({
      case (ts, data) =>
        existingOp.apply(data).map { vals =>
          vals.map {
            case (k, v) =>
              ((k, batcher.batchOf(ts)), (ts, v))
          }
        }
    })

  def wrapTime[T, U, O](
    existingOp: FlatMapOperation[T, U],
    finalTransform: (Item[U]) => O
  ): FlatMapOperation[Item[T], O] = {
    FlatMapOperation.generic({ x: (Timestamp, T) =>
      existingOp.apply(x._2).map { vals =>
        vals.map(finalTransform(x._1, _))
      }
    })
  }
}

private[storm] case class FlatMapBoltProvider(
  builder: StormTopologyBuilder,
  node: FlatMapNode[Storm]
) extends ComponentProvider {
  import FlatMapBoltProvider._
  import Producer2FlatMapOperation._

  override def createSingle[T, O](fn: Item[T] => O): Topology.Component[O] =
    itemBolt[Any, T, O](fn)

  override def createAggregated[K, V](
    batcher: Batcher,
    shards: KeyValueShards,
    semigroup: Semigroup[V]
  ): Option[Topology.Component[Aggregated[K, V]]] =
    Some(aggregatedBolt[Any, K, V](batcher, shards, semigroup))

  private def getOrElse[T <: AnyRef: ClassTag](default: T, queryNode: StormNode = node) =
    builder.getOrElse(queryNode, default)

  // Boilerplate extracting of the options from the DAG
  private val nodeName = builder.getNodeName(node)
  private val metrics = getOrElse(DEFAULT_FM_STORM_METRICS)
  private val anchorTuples = getOrElse(AnchorTuples.default)
  logger.info(s"[$nodeName] Anchoring: ${anchorTuples.anchor}")

  private val maxWaiting = getOrElse(DEFAULT_MAX_WAITING_FUTURES)
  private val maxWaitTime = getOrElse(DEFAULT_MAX_FUTURE_WAIT_TIME)
  logger.info(s"[$nodeName] maxWaiting: ${maxWaiting.get}")

  private val ackOnEntry = getOrElse(DEFAULT_ACK_ON_ENTRY)
  logger.info(s"[$nodeName] ackOnEntry : ${ackOnEntry.get}")

  private val maxExecutePerSec = getOrElse(DEFAULT_MAX_EXECUTE_PER_SEC)
  logger.info(s"[$nodeName] maxExecutePerSec : $maxExecutePerSec")

  private val maxEmitPerExecute = getOrElse(DEFAULT_MAX_EMIT_PER_EXECUTE)
  logger.info(s"[$nodeName] maxEmitPerExecute : ${maxEmitPerExecute.get}")

  private val parallelism = getOrElse(DEFAULT_FM_PARALLELISM)
  logger.info(s"[$nodeName] parallelism: ${parallelism.parHint}")

  private def aggregatedBolt[T, K, V](
    batcher: Batcher,
    shards: KeyValueShards,
    semigroup: Semigroup[V]
  ): Topology.Bolt[Item[T], Aggregated[K, V]] = {
    // When emitting tuples between the Final Flat Map and the summer we encode the timestamp in the value
    // The monoid we use in aggregation is timestamp max.
    implicit val valueMonoid: Semigroup[V] = semigroup

    val operation = foldOperations[T, (K, V)](node.members.reverse)
    val wrappedOperation = wrapTimeBatchIDKV(operation)(batcher)

    val summerBuilder = BuildSummer(builder, nodeName, node.lastProducer.get)

    Topology.Bolt(
      parallelism.parHint,
      metrics.metrics,
      anchorTuples,
      ackOnEntry,
      maxExecutePerSec,
      new executor.FinalFlatMap(
        wrappedOperation,
        summerBuilder,
        maxWaiting,
        maxWaitTime,
        maxEmitPerExecute,
        shards
      )(implicitly[Semigroup[AggregateValue[V]]])
    )
  }

  private def itemBolt[T, U, O](finalTransform: (Item[U]) => O): Topology.Bolt[Item[T], O] = {
    val operation = foldOperations[T, U](node.members.reverse)
    val wrappedOperation = wrapTime(operation, finalTransform)

    Topology.Bolt(
      parallelism.parHint,
      metrics.metrics,
      anchorTuples,
      ackOnEntry,
      maxExecutePerSec,
      new executor.IntermediateFlatMap(
        wrappedOperation,
        maxWaiting,
        maxWaitTime,
        maxEmitPerExecute
      )
    )
  }
}
