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
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple

import com.twitter.algebird.{Semigroup, Monoid}
import com.twitter.summingbird._
import com.twitter.summingbird.chill._
import com.twitter.summingbird.batch.{BatchID, Batcher, Timestamp}
import com.twitter.summingbird.storm.option.{AckOnEntry, AnchorTuples}
import com.twitter.summingbird.online.executor.InputState
import com.twitter.summingbird.online.option.{IncludeSuccessHandler, MaxWaitingFutures, MaxFutureWaitTime, SummerBuilder}
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.planner._
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.storm.planner._
import org.slf4j.LoggerFactory

object FlatMapBoltProvider {
  @transient private val logger = LoggerFactory.getLogger(FlatMapBoltProvider.getClass)
  private def wrapTimeBatchIDKV[T, K, V](existingOp: FlatMapOperation[T, (K, V)])(batcher: Batcher):
                          FlatMapOperation[(Timestamp, T), ((K, BatchID), (Timestamp, V))] = {
      FlatMapOperation.generic({case (ts: Timestamp, data: T) =>
          existingOp.apply(data).map { vals =>
            vals.map{ tup =>
              ((tup._1, batcher.batchOf(ts)), (ts, tup._2))
            }
          }
      })
    }

  def wrapTime[T, U](existingOp: FlatMapOperation[T, U]): FlatMapOperation[(Timestamp, T), (Timestamp, U)] = {
      FlatMapOperation.generic({x: (Timestamp, T) =>
          existingOp.apply(x._2).map { vals =>
            vals.map((x._1, _))
          }
      })
    }
}

case class FlatMapBoltProvider(storm: Storm, jobID: SummingbirdJobId, stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) {
  import FlatMapBoltProvider._

  def getOrElse[T <: AnyRef : Manifest](default: T, queryNode: StormNode = node) = storm.getOrElse(stormDag, queryNode, default)
  /**
     * Keep the crazy casts localized in here
     */
  private def foldOperations[T, U](producers: List[Producer[Storm, _]]): FlatMapOperation[T, U] =
    producers.foldLeft(FlatMapOperation.identity[Any]) {
      case (acc, p) =>
        p match {
          case LeftJoinedProducer(_, wrapper) =>
            val newService = wrapper.store
            FlatMapOperation.combine(
              acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
              newService.asInstanceOf[StoreFactory[Any, Any]]).asInstanceOf[FlatMapOperation[Any, Any]]
          case OptionMappedProducer(_, op) => acc.andThen(FlatMapOperation[Any, Any](op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]]))
          case FlatMappedProducer(_, op) => acc.andThen(FlatMapOperation(op).asInstanceOf[FlatMapOperation[Any, Any]])
          case WrittenProducer(_, sinkSupplier) =>
            acc.andThen(FlatMapOperation.write(() => sinkSupplier.toFn))
          case IdentityKeyedProducer(_) => acc
          case MergedProducer(_, _) => acc
          case NamedProducer(_, _) => acc
          case AlsoProducer(_, _) => acc
          case Source(_) => sys.error("Should not schedule a source inside a flat mapper")
          case Summer(_, _, _) => sys.error("Should not schedule a Summer inside a flat mapper")
          case KeyFlatMappedProducer(_, op) => acc.andThen(FlatMapOperation.keyFlatMap[Any, Any, Any](op).asInstanceOf[FlatMapOperation[Any, Any]])
        }
    }.asInstanceOf[FlatMapOperation[T, U]]

  // Boilerplate extracting of the options from the DAG
  private val nodeName = stormDag.getNodeName(node)
  private val metrics = getOrElse(DEFAULT_FM_STORM_METRICS)
  private val anchorTuples = getOrElse(AnchorTuples.default)
  logger.info("[{}] Anchoring: {}", nodeName, anchorTuples.anchor)

  private val maxWaiting = getOrElse(DEFAULT_MAX_WAITING_FUTURES)
  private val maxWaitTime = getOrElse(DEFAULT_MAX_FUTURE_WAIT_TIME)
  logger.info("[{}] maxWaiting: {}", nodeName, maxWaiting.get)

  private val flushFrequency = getOrElse(DEFAULT_FLUSH_FREQUENCY)
  logger.info("[{}] maxWaiting: {}", nodeName, flushFrequency.get)

  private val cacheSize = getOrElse(DEFAULT_FM_CACHE)
  logger.info("[{}] cacheSize lowerbound: {}", nodeName, cacheSize.lowerBound)

  private val useAsyncCache = getOrElse(DEFAULT_USE_ASYNC_CACHE)
  logger.info("[{}] useAsyncCache : {}", nodeName, useAsyncCache.get)

  private val ackOnEntry = getOrElse(DEFAULT_ACK_ON_ENTRY)
  logger.info("[{}] ackOnEntry : {}", nodeName, ackOnEntry.get)

  private val maxEmitPerExecute = getOrElse(DEFAULT_MAX_EMIT_PER_EXECUTE)
  logger.info("[{}] maxEmitPerExecute : {}", nodeName, maxEmitPerExecute.get)

  private def getFFMBolt[T, K, V](summer: SummerNode[Storm]) = {
    type ExecutorInput = (Timestamp, T)
    type ExecutorKey = Int
    type InnerValue = (Timestamp, V)
    type ExecutorValue = Map[(K, BatchID), InnerValue]
    val summerProducer = summer.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Storm, K, V]]
    // When emitting tuples between the Final Flat Map and the summer we encode the timestamp in the value
    // The monoid we use in aggregation is timestamp max.
    val batcher = summerProducer.store.batcher
    implicit val valueMonoid: Semigroup[V] = summerProducer.semigroup

    // Query to get the summer paralellism of the summer down stream of us we are emitting to
    // to ensure no edge case between what we might see for its parallelism and what it would see/pass to storm.
    val summerParalellism = getOrElse(DEFAULT_SUMMER_PARALLELISM, summer)
    val summerBatchMultiplier = getOrElse(DEFAULT_SUMMER_BATCH_MULTIPLIER, summer)

    // This option we report its value here, but its not user settable.
    val keyValueShards = executor.KeyValueShards(summerParalellism.parHint * summerBatchMultiplier.get)
    logger.info("[{}] keyValueShards : {}", nodeName, keyValueShards.get)

    val operation = foldOperations[T, (K, V)](node.members.reverse)
    val wrappedOperation = wrapTimeBatchIDKV(operation)(batcher)

    val builder = BuildSummer(storm, stormDag, node)

    BaseBolt(
      jobID,
      metrics.metrics,
      anchorTuples,
      true,
      new Fields(AGG_KEY, AGG_VALUE),
      ackOnEntry,
      new executor.FinalFlatMap(
        wrappedOperation,
        builder,
        maxWaiting,
        maxWaitTime,
        maxEmitPerExecute,
        keyValueShards,
        new SingleItemInjection[ExecutorInput],
        new KeyValueInjection[ExecutorKey, ExecutorValue]
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
      new executor.IntermediateFlatMap(
        wrappedOperation,
        maxWaiting,
        maxWaitTime,
        maxEmitPerExecute,
        new SingleItemInjection[ExecutorInput],
        new SingleItemInjection[ExecutorOutput]
        )
    )
  }

  def apply: BaseBolt[Any, Any] = {
    val summerOpt:Option[SummerNode[Storm]] = stormDag.dependantsOf(node).collect{case s: SummerNode[Storm] => s}.headOption
    summerOpt match {
      case Some(s) => getFFMBolt[Any, Any, Any](s).asInstanceOf[BaseBolt[Any, Any]]
      case None => getIntermediateFMBolt[Any, Any].asInstanceOf[BaseBolt[Any, Any]]
    }
  }
}
