package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.{ Options, Summer }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, FlatMapNode, SourceNode, SummerNode }
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.builder.Topology
import com.twitter.summingbird.storm.planner.StormNode
import org.apache.storm.generated.StormTopology
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag
import scala.collection.{ Map => CMap }

/**
 * This class contains main logic on how to build storm topology out of planned DAG.
 * `DAG` contains three types of nodes: Source, FlatMap and Summer.
 * Built topology has next properties:
 * 1. separate `Topology` `Component`s corresponds to separate DAG nodes
 * 2. each `Component` emit values only of one type
 * 3.a. `FlatMap` components accept tuples of type `Item[T]`
 *      (and corresponds to `Producer[Storm, T]`)
 * 3.b. `Summer` components accept tuples of type `SummerInput[K, V]`
 *      (and corresponds to `KeyedProducer[Storm, K, V]`)
 * 4. All components emit some of those: `Item[T]`, `KeyValue[K, V]`, `Aggregated[K, V]` or `Sharded[K, V]`.
 * 4.a. If corresponding to `Component` `Producer` is not `Keyed` then `Component` emits `Item[T]`
 * 4.b. Otherwise if `Component` emits only to `Summer` nodes it emits `Aggregated[K, V]`
 * 4.c. Otherwise if `Component` emits to both `Summer` and non `Summer` nodes it emits `Sharded[K, V]`
 *      which can be both grouped in the same way as `Aggregated` and shuffled in the same way as `Item[T]`
 * 4.d. If `Component` emits `Keyed` values but there aren't downstream `Summer` nodes it emits `Item[T]`,
 *      but this will be changed to `KeyValue[K, V]` with grouped `leftJoin` feature.
 */
private[storm] object StormTopologyBuilder {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormTopologyBuilder])

  type Item[T] = (Timestamp, T)
  type KeyValue[K, V] = (Timestamp, K, V)

  type AggregateKey[K] = (K, BatchID)
  type AggregateValue[V] = (Timestamp, V)
  type Aggregated[K, V] = (Int, CMap[AggregateKey[K], AggregateValue[V]])
  type Sharded[K, V] = (Int, AggregateKey[K], AggregateValue[V])

  type SummerInput[K, V] = Traversable[(AggregateKey[K], AggregateValue[V])]

  type FlatMapBoltId[T] = Topology.BoltId[Item[T], _]
  type SummerBoltId[K, V] = Topology.BoltId[SummerInput[K, V], _]

  private case class OutgoingSummersProps(
    allSummers: Boolean,
    batcher: Batcher,
    semigroup: Semigroup[_],
    shards: KeyValueShards
  )

  private def wrapKeyValue[K, V](tuple: Item[(K, V)]): KeyValue[K, V] =
    (tuple._1, tuple._2._1, tuple._2._2)

  private def wrapSharded[K, V](
    batcher: Batcher,
    shards: KeyValueShards
  ): (Item[(K, V)] => Sharded[K, V]) = { tuple =>
    val wrappedKey = (tuple._2._1, batcher.batchOf(tuple._1))
    val wrappedValue = (tuple._1, tuple._2._2)
    val shardId = shards.summerIdFor(wrappedKey)
    (shardId, wrappedKey, wrappedValue)
  }
}

/**
 * This class encapsulates logic how to build `StormTopology` from DAG of the job, jobId and options.
 */
private[storm] case class StormTopologyBuilder(options: Map[String, Options], jobId: JobId, stormDag: Dag[Storm]) {
  import StormTopologyBuilder._

  def build: StormTopology = {
    val topology = stormDag.nodes.foldLeft(Topology.empty) {
      case (currentTopology, node: SummerNode[Storm]) =>
        register(currentTopology, node, SummerBoltProvider(this, node))
      case (currentTopology, node: FlatMapNode[Storm]) =>
        register(currentTopology, node, FlatMapBoltProvider(this, node))
      case (currentTopology, node: SourceNode[Storm]) =>
        register(currentTopology, node, SpoutProvider(this, node))
    }
    topology.build(jobId)
  }

  private def register(topology: Topology, node: StormNode, provider: ComponentProvider): Topology =
    if (!shouldEmitKeyValues(node)) {
      registerItemComponent(topology, node, provider)
    } else {
      registerKeyValueComponent(topology, node, provider)
    }

  private def registerItemComponent[T](
    topology: Topology,
    node: StormNode,
    provider: ComponentProvider
  ): Topology = {
    val component = provider.createSingle(identity[Item[T]])
    val (componentId, topologyWithComponent) = topology.withComponent(getNodeName(node), component)
    registerItemEdges[T](topologyWithComponent, node, componentId)
  }

  private def registerKeyValueComponent[K, V](
    topology: Topology,
    node: StormNode,
    provider: ComponentProvider
  ): Topology = outgoingSummersProps(node) match {
    case Some(props) if props.allSummers =>
      // Use Aggregated if possible.
      provider.createAggregated[K, V](props.batcher, props.shards, props.semigroup
        .asInstanceOf[Semigroup[V]]) match {
        case Some(component) =>
          val (componentId, topologyWithComponent) = topology.withComponent(getNodeName(node), component)
          registerAggregatedEdges[K, V](topologyWithComponent, node, componentId)
        case None =>
          // Fallback to Sharded.
          registerShardedKeyValue[K, V](topology, node, provider, props.batcher, props.shards)
      }
    case Some(props) =>
      registerShardedKeyValue[K, V](topology, node, provider, props.batcher, props.shards)
    case None =>
      // There is no outgoing summers - use plain key value edge.
      val component = provider.createSingle[(K, V), KeyValue[K, V]](wrapKeyValue)
      val (componentId, topologyWithComponent) = topology.withComponent(getNodeName(node), component)
      registerKeyValueEdges[K, V](topologyWithComponent, node, componentId)
  }

  private def registerShardedKeyValue[K, V](
    topology: Topology,
    node: StormNode,
    provider: ComponentProvider,
    batcher: Batcher,
    shards: KeyValueShards
  ): Topology = {
    val component = provider.createSingle[(K, V), Sharded[K, V]](wrapSharded(batcher, shards))
    val (componentId, topologyWithComponent) = topology.withComponent(getNodeName(node), component)
    registerShardedEdges[K, V](topologyWithComponent, node, componentId)
  }

  private def registerItemEdges[T](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[Item[T]]
  ): Topology = stormDag.dependantsOf(source).foldLeft(topology) {
    case (currentTopology, downstreamNode: FlatMapNode[Storm]) =>
      currentTopology.withEdge(Edges.shuffleItemToItem[T](
        sourceId,
        getFMBoltId[T](downstreamNode),
        withLocalGrouping(downstreamNode)
      ))
    case (_, downstreamNode: SummerNode[Storm]) =>
      throw new Exception(s"Impossible to create item edge to summer node: " +
        s"$sourceId -> ${getNodeName(downstreamNode)}")
  }

  private def registerKeyValueEdges[K, V](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[KeyValue[K, V]]
  ): Topology = stormDag.dependantsOf(source).foldLeft(topology) {
    case (currentTopology, downstreamNode: FlatMapNode[Storm]) =>
      currentTopology.withEdge(Edges.shuffleKeyValueToItem[K, V](
        sourceId,
        getFMBoltId[(K, V)](downstreamNode),
        withLocalGrouping(downstreamNode)
      ))
    case (_, downstreamNode: SummerNode[Storm]) =>
      throw new Exception(s"Impossible to create key value edge to summer node: " +
        s"$sourceId -> ${getNodeName(downstreamNode)}")
  }

  private def registerAggregatedEdges[K, V](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[Aggregated[K, V]]
  ): Topology = stormDag.dependantsOf(source).foldLeft(topology) {
    case (_, downstreamNode: FlatMapNode[Storm]) =>
      throw new Exception(s"Impossible to create aggregated edge to flat map node: " +
        s"$sourceId -> ${ getNodeName(downstreamNode) }")
    case (currentTopology, downstreamNode: SummerNode[Storm]) =>
      currentTopology.withEdge(Edges.groupedAggregatedToSummer[K, V](
        sourceId,
        getSummerBoltId[K, V](downstreamNode)
      ))
  }

  private def registerShardedEdges[K, V](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[Sharded[K, V]]
  ): Topology = stormDag.dependantsOf(source).foldLeft(topology) {
    case (currentTopology, downstreamNode: FlatMapNode[Storm]) =>
      currentTopology.withEdge(Edges.shuffleShardedToItem[K, V](
        sourceId,
        getFMBoltId[(K, V)](downstreamNode),
        withLocalGrouping(downstreamNode)
      ))
    case (currentTopology, downstreamNode: SummerNode[Storm]) =>
      currentTopology.withEdge(Edges.groupedShardedToSummer[K, V](
        sourceId,
        getSummerBoltId[K, V](downstreamNode)
      ))
  }

  private def getSummerBoltId[K, V](node: SummerNode[Storm]): SummerBoltId[K, V] =
    Topology.BoltId(getNodeName(node))

  private def getFMBoltId[T](node: FlatMapNode[Storm]): FlatMapBoltId[T] =
    Topology.BoltId(getNodeName(node))

  private def shouldEmitKeyValues(node: StormNode): Boolean =
    stormDag.dependantsOf(node).exists(_.isInstanceOf[SummerNode[_]])

  private def withLocalGrouping(node: FlatMapNode[Storm]): Boolean = {
    val usePreferLocalDependency = getOrElse(node, DEFAULT_FM_PREFER_LOCAL_DEPENDENCY)
    logger.info(s"[${getNodeName(node)}] usePreferLocalDependency: ${usePreferLocalDependency.get}")
    usePreferLocalDependency.get
  }

  private def outgoingSummersProps(node: StormNode): Option[OutgoingSummersProps] = {
    val dependants = stormDag.dependantsOf(node)
    val summerNodes = dependants.collect { case s: SummerNode[Storm] => s }
    if (summerNodes.nonEmpty) {
      val shards = getSummerKeyValueShards(summerNodes.head)
      val batcher = getSummer(summerNodes.head).store.mergeableBatcher
      val semigroup = getSummer(summerNodes.head).semigroup

      summerNodes.foreach { summerNode =>
        assert(
          getSummerKeyValueShards(summerNode) == shards &&
          getSummer(summerNode).store.mergeableBatcher == batcher &&
          getSummer(summerNode).semigroup == semigroup,
          "All outgoing summers should have same number of shards, batcher and semigroup. " +
            "See https://github.com/twitter/summingbird/issues/733 for details."
        )
      }

      Some(OutgoingSummersProps(summerNodes.size == dependants.size, batcher, semigroup, shards))
    } else {
      None
    }
  }

  def getSummer(node: SummerNode[Storm]): Summer[Storm, _, _] = node.members
    .collect { case s: Summer[Storm, _, _] => s }.head

  private def getSummerKeyValueShards(summer: SummerNode[Storm]): executor.KeyValueShards = {
    // Query to get the summer paralellism of the summer down stream of us we are emitting to
    // to ensure no edge case between what we might see for its parallelism and what it would see/pass to storm.
    val summerParalellism = getOrElse(summer, DEFAULT_SUMMER_PARALLELISM)
    val summerBatchMultiplier = getOrElse(summer, DEFAULT_SUMMER_BATCH_MULTIPLIER)

    executor.KeyValueShards(summerParalellism.parHint * summerBatchMultiplier.get)
  }

  private[storm] def getOrElse[T <: AnyRef: ClassTag](node: StormNode, default: T): T =
    get[T](node) match {
      case None =>
        logger.debug(s"Node (${getNodeName(node)}): Using default setting $default")
        default
      case Some((namedSource, option)) =>
        logger.info(s"Node ${getNodeName(node)}: Using $option found via NamedProducer ${'"'}$namedSource${'"'}")
        option
    }

  private[storm] def get[T <: AnyRef: ClassTag](node: StormNode): Option[(String, T)] = {
    val producer = node.members.last
    Options.getFirst[T](options, stormDag.producerToPriorityNames(producer))
  }

  private[storm] def getNodeName(node: StormNode) = stormDag.getNodeName(node)
}
