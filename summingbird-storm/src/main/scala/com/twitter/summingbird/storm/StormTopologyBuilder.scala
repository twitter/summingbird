package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.{ LeftJoinedProducer, Options, Producer, Summer }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.{ Grouping, LeftJoinGrouping }
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
 * DAG contains three types of nodes: [[SourceNode]], [[FlatMapNode]] and [[SummerNode]].
 * Built topology has next properties:
 * 1. separate [[Topology.Component]]'s corresponds to separate DAG nodes
 * 2. each [[Topology.Component]] emit values only of one type
 * 3.a. [[FlatMapNode]] components accept tuples of type [[ Item[T] ]]
 *      (and correspond to [[ Producer[Storm, T] ]])
 * 3.b. [[SummerNode]] components accept tuples of type [[ SummerInput[K, V] ]]
 *      (and correspond to [[ KeyedProducer[Storm, K, V] ]])
 * 4. All components emit some of those: [[ Item[T] ]], [[ Aggregated[K, V] ]] or [[ Sharded[K, V] ]]
 * 4.a. If corresponding to [[Topology.Component]] [[com.twitter.summingbird.Producer]] is not
 *      [[com.twitter.summingbird.KeyedProducer]] then [[Topology.Component]] emits [[ Item[T] ]]
 * 4.b. Otherwise if [[Topology.Component]] emits only to [[SummerNode]] nodes it emits [[ Aggregated[K, V] ]]
 * 4.c. Otherwise if [[Topology.Component]] emits to both [[SummerNode]] and [[FlatMapNode]] nodes
 *      it emits [[ Sharded[K, V] ]] which can be both grouped in the same way as [[ Aggregated[K, V] ]]
 *      and shuffled in the same way as [[ Item[T] ]].
 */
private[storm] object StormTopologyBuilder {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormTopologyBuilder])

  // Type to represent simplest values storm components emit.
  // Corresponds to plain [[ Producer[Storm, T] ]] case.
  type Item[T] = (Timestamp, T)

  // Types to represent aggregate keys and values components emit in case of partial aggregation.
  type AggregateKey[K] = (K, BatchID)
  type AggregateValue[V] = (Timestamp, V)
  type Aggregated[K, V] = (Int, CMap[AggregateKey[K], AggregateValue[V]])

  // Type to represent key value pair, but which possible to use togetger with Aggregated.
  type Sharded[K, V] = (Int, AggregateKey[K], AggregateValue[V])

  // Type to represent input for [[SummerNode]] components.
  type SummerInput[K, V] = Iterable[(AggregateKey[K], AggregateValue[V])]

  // Types to represent ids of bolts corresponding to [[FlatMapNode]] and [[SummerNode]].
  type FlatMapBoltId[T] = Topology.BoltId[Item[T], _]
  type SummerBoltId[K, V] = Topology.BoltId[SummerInput[K, V], _]

  def itemToSharded[K, V](
    batcher: Batcher,
    shards: KeyValueShards
  ): (Item[(K, V)] => Sharded[K, V]) = {
    case (timestamp, (key, value)) =>
      val aggregateKey = (key, batcher.batchOf(timestamp))
      val aggregateValue = (timestamp, value)
      val shardId = shards.summerIdFor(aggregateKey)
      (shardId, aggregateKey, aggregateValue)
  }

  def aggregatedToSummerInput[K, V]: (Aggregated[K, V] => SummerInput[K, V]) = {
    case (shardId, keyValues) => keyValues
  }

  def shardedToItem[K, V]: (Sharded[K, V] => Item[(K, V)]) = {
    case (shardId, (key, batchId), (timestamp, value)) => (timestamp, (key, value))
  }

  def shardedToSummerInput[K, V]: (Sharded[K, V] => SummerInput[K, V]) = {
    case (shardId, aggregateKey, aggregateValue) => Some((aggregateKey, aggregateValue))
  }

  private case class OutgoingSummersProps(
    allSummers: Boolean,
    batcher: Batcher,
    semigroup: Semigroup[_],
    shards: KeyValueShards
  )
}

/**
 * This class encapsulates logic how to build [[StormTopology]] from DAG of the job, jobId and options.
 */
private[storm] case class StormTopologyBuilder(options: Map[String, Options], jobId: JobId, stormDag: Dag[Storm]) {
  import StormTopologyBuilder._

  def build: Topology = stormDag.nodes.foldLeft(Topology.empty) {
    case (currentTopology, node: SummerNode[Storm]) =>
      register(currentTopology, node, SummerBoltProvider(this, node))
    case (currentTopology, node: FlatMapNode[Storm]) =>
      register(currentTopology, node, FlatMapBoltProvider(this, node))
    case (currentTopology, node: SourceNode[Storm]) =>
      register(currentTopology, node, SpoutProvider(this, node))
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
      // Use [[Aggregated]] if possible.
      provider.createAggregated[K, V](
        props.batcher,
        props.shards,
        props.semigroup.asInstanceOf[Semigroup[V]]
      ) match {
        case Some(component) =>
          val (componentId, topologyWithComponent) = topology.withComponent(getNodeName(node), component)
          registerAggregatedEdges[K, V](topologyWithComponent, node, componentId)
        case None =>
          // Fallback to [[Sharded]], this happens if component doesn't support aggregation on emitted values,
          // for example in case of [[SummerBoltProvider]].
          registerShardedKeyValue[K, V](topology, node, provider, props.batcher, props.shards)
      }
    case Some(props) =>
      registerShardedKeyValue[K, V](topology, node, provider, props.batcher, props.shards)
    case None =>
      registerKeyValue[K, V](topology, node, provider)
  }

  private def registerKeyValue[K, V](
    topology: Topology,
    node: StormNode,
    provider: ComponentProvider
  ): Topology = {
    val component = provider.createSingle[(K, V), Item[(K, V)]](identity)
    val (componentId, topologyWithComponent) = topology.withComponent(getNodeName(node), component)
    registerKeyValueItemEdges(topologyWithComponent, node, componentId)
  }

  private def registerKeyValueItemEdges[K, V](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[Item[(K, V)]]
  ): Topology = stormDag.dependantsOf(source).foldLeft(topology) {
    case (currentTopology, downstreamNode: FlatMapNode[Storm]) =>
      val destId = getFMBoltId[(K, V)](downstreamNode)
      val edge = if (isGroupedLeftJoinNode(downstreamNode)) {
        Edges.groupedKeyValueItemToItem[K, V](sourceId, destId)
      } else {
        Edges.shuffleItemToItem[(K, V)](sourceId, destId, withLocalGrouping(downstreamNode))
      }
      currentTopology.withEdge(edge)
    case (_, downstreamNode: SummerNode[Storm]) =>
      throw new IllegalStateException(s"Impossible to create key value edge to summer node: " +
        s"$sourceId -> ${getNodeName(downstreamNode)}")
  }

  private def registerShardedKeyValue[K, V](
    topology: Topology,
    node: StormNode,
    provider: ComponentProvider,
    batcher: Batcher,
    shards: KeyValueShards
  ): Topology = {
    val component = provider.createSingle[(K, V), Sharded[K, V]](itemToSharded(batcher, shards))
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
      throw new IllegalStateException(s"Impossible to create item edge to summer node: " +
        s"$sourceId -> ${getNodeName(downstreamNode)}")
  }

  private def registerAggregatedEdges[K, V](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[Aggregated[K, V]]
  ): Topology = stormDag.dependantsOf(source).foldLeft(topology) {
    case (_, downstreamNode: FlatMapNode[Storm]) =>
      throw new IllegalStateException(s"Impossible to create aggregated edge to flat map node: " +
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
      val destId = getFMBoltId[(K, V)](downstreamNode)
      val edge = if (isGroupedLeftJoinNode(downstreamNode)) {
        Edges.groupedShardedToItem[K, V](sourceId, destId)
      } else {
        Edges.shuffleShardedToItem[K, V](sourceId, destId, withLocalGrouping(downstreamNode))
      }
      currentTopology.withEdge(edge)
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
    stormDag.dependantsOf(node).exists {
      case flatMapNode: FlatMapNode[Storm] => isGroupedLeftJoinNode(flatMapNode)
      case summerNode: SummerNode[Storm] => true
    }

  private def withLocalGrouping(node: FlatMapNode[Storm]): Boolean = {
    val usePreferLocalDependency = getOrElse(node, DEFAULT_FM_PREFER_LOCAL_DEPENDENCY)
    logger.info(s"[${getNodeName(node)}] usePreferLocalDependency: ${usePreferLocalDependency.get}")
    usePreferLocalDependency.get
  }

  private def isGroupedLeftJoinNode(node: FlatMapNode[Storm]): Boolean =
    node.firstProducer.map { producer =>
      producer.isInstanceOf[LeftJoinedProducer[Storm, _, _, _]] &&
        get[LeftJoinGrouping](node).map { case (_, leftJoinGrouping) =>
          leftJoinGrouping.get
        } == Some(Grouping.Group)
    }.getOrElse(false)

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

  private[storm] def get[T <: AnyRef: ClassTag](node: StormNode): Option[(String, T)] =
    node.firstProducer.flatMap { producer => get(producer) }

  private [storm] def get[T <: AnyRef: ClassTag](producer: Producer[Storm, _]): Option[(String, T)] =
    Options.getFirst[T](options, stormDag.producerToPriorityNames(producer))

  private[storm] def getNodeName(node: StormNode) = stormDag.getNodeName(node)
}
