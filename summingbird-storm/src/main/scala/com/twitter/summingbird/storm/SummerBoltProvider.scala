package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.Summer
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.{ FlatMapOperation, MergeableStoreFactory, WrappedTSInMergeable, executor }
import com.twitter.summingbird.online.option.IncludeSuccessHandler
import com.twitter.summingbird.planner.SummerNode
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.StormTopologyBuilder._
import com.twitter.summingbird.storm.builder.Topology
import com.twitter.summingbird.storm.option.AnchorTuples
import com.twitter.summingbird.storm.planner.StormNode
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

private[storm] case object SummerBoltProvider {
  @transient private val logger = LoggerFactory.getLogger(SummerBoltProvider.getClass)
}

private[storm] case class SummerBoltProvider(
  builder: StormTopologyBuilder,
  node: SummerNode[Storm]
) extends ComponentProvider {
  import SummerBoltProvider._

  override def createSingle[T, O](fn: Item[T] => O): Topology.Component[O] =
    // This is a legitimate conversion because we know that summer emits `(K, (Option[V], V))`.
    bolt[Any, Any, O](fn.asInstanceOf[Item[(Any, (Option[Any], Any))] => O])

  override def createAggregated[K, V](
    batcher: Batcher,
    shards: KeyValueShards,
    semigroup: Semigroup[V]
  ): Option[Topology.Component[Aggregated[K, V]]] =
  // There is no way to do partial aggregation in summer node.
    None

  private def bolt[K, V, O](
    finalTransform: Item[(K, (Option[V], V))] => O
  ): Topology.Bolt[SummerInput[K, V], O] = {
    val nodeName = builder.getNodeName(node)

    val summer = node.members.collect { case c@Summer(_, _, _) => c }.head
      .asInstanceOf[Summer[Storm, K, V]]

    implicit val semigroup = summer.semigroup
    implicit val batcher = summer.store.mergeableBatcher

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

    val summerBuilder = BuildSummer(builder, nodeName, summer)
    val storeBaseFMOp: ((AggregateKey[K], (Option[Item[V]], Item[V]))) => TraversableOnce[O] = {
      case ((key, batchID), (optPrevExecutorValue, (timestamp, value))) =>
        val optPrevValue = optPrevExecutorValue.map(_._2)
        Some(finalTransform(timestamp, (key, (optPrevValue, value))))
    }
    val flatmapOp: FlatMapOperation[(AggregateKey[K], (Option[Item[V]], Item[V])), O] =
      FlatMapOperation.apply(storeBaseFMOp)

    val supplier: MergeableStoreFactory[AggregateKey[K], V] = summer.store

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
