package com.twitter.summingbird.storm

import com.twitter.summingbird.Summer
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.online.{ FlatMapOperation, MergeableStoreFactory, WrappedTSInMergeable, executor }
import com.twitter.summingbird.online.option.IncludeSuccessHandler
import com.twitter.summingbird.planner.SummerNode
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.builder.Topology
import com.twitter.summingbird.storm.option.AnchorTuples
import com.twitter.summingbird.storm.planner.StormNode
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag
import scala.collection.{ Map => CMap }

case object SummerBoltProvider {
  @transient private val logger = LoggerFactory.getLogger(SummerBoltProvider.getClass)
}

case class SummerBoltProvider(builder: StormTopologyBuilder, node: SummerNode[Storm]) {
  import SummerBoltProvider._

  def apply[K, V]: Topology.Bolt[
    (Int, CMap[(K, BatchID), (Timestamp, V)]),
    (Timestamp, (K, (Option[V], V)))] = {
    val nodeName = builder.getNodeName(node)

    val summer = node.members.collect { case c@Summer(_, _, _) => c }.head
      .asInstanceOf[Summer[Storm, K, V]]

    implicit val semigroup = summer.semigroup
    implicit val batcher = summer.store.mergeableBatcher

    type ExecutorKeyType = (K, BatchID)
    type ExecutorValueType = (Timestamp, V)
    type ExecutorOutputType = (Timestamp, (K, (Option[V], V)))

    val anchorTuples = getOrElse(AnchorTuples.default)
    val metrics = getOrElse(DEFAULT_SUMMER_STORM_METRICS)

    val ackOnEntry = getOrElse(DEFAULT_ACK_ON_ENTRY)
    logger.info(s"[$nodeName] ackOnEntry : ${ackOnEntry.get}")

    val maxEmitPerExecute = getOrElse(DEFAULT_MAX_EMIT_PER_EXECUTE)
    logger.info(s"[$nodeName] maxEmitPerExecute : ${maxEmitPerExecute.get}")

    val maxExecutePerSec = getOrElse(DEFAULT_MAX_EXECUTE_PER_SEC)
    logger.info(s"[$nodeName] maxExecutePerSec : $maxExecutePerSec")

    val parallelism = getOrElse(DEFAULT_SUMMER_PARALLELISM).parHint
    logger.info(s"[$nodeName] parallelism : $parallelism")

    val summerBuilder = BuildSummer(builder, node)
    val storeBaseFMOp = { op: (ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)) =>
      val ((key, batchID), (optPrevExecutorValue, (timestamp, value))) = op
      val optPrevValue = optPrevExecutorValue.map(_._2)
      List((timestamp, (key, (optPrevValue, value))))
    }

    val flatmapOp: FlatMapOperation[(ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)), ExecutorOutputType] =
      FlatMapOperation.apply(storeBaseFMOp)

    val supplier: MergeableStoreFactory[ExecutorKeyType, V] = summer.store

    Topology.Bolt(
      parallelism,
      metrics.metrics,
      anchorTuples,
      ackOnEntry,
      maxExecutePerSec,
      new executor.Summer(
        () => new WrappedTSInMergeable(supplier.mergeableStore(semigroup)),
        flatmapOp,
        getOrElse(DEFAULT_ONLINE_SUCCESS_HANDLER),
        getOrElse(DEFAULT_ONLINE_EXCEPTION_HANDLER),
        summerBuilder,
        getOrElse(DEFAULT_MAX_WAITING_FUTURES),
        getOrElse(DEFAULT_MAX_FUTURE_WAIT_TIME),
        maxEmitPerExecute,
        getOrElse(IncludeSuccessHandler.default))
    )
  }

  private def getOrElse[T <: AnyRef: ClassTag](default: T, queryNode: StormNode = node) =
    builder.getOrElse(queryNode, default)
}
