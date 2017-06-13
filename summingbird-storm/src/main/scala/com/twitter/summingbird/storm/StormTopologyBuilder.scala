package com.twitter.summingbird.storm

import com.twitter.summingbird.Options
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, FlatMapNode, SourceNode, SummerNode }
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.builder.{ EdgeType, Topology }
import com.twitter.summingbird.storm.planner.StormNode
import org.apache.storm.generated.StormTopology
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

import scala.collection.{ Map => CMap }

object StormTopologyBuilder {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormTopologyBuilder])

  type Item[T] = (Timestamp, T)

  type AggregateKey[K] = (K, BatchID)
  type AggregateValue[V] = (Timestamp, V)
  type Aggregated[K, V] = (Int, CMap[AggregateKey[K], AggregateValue[V]])

  type SummerInput[K, V] = Traversable[(AggregateKey[K], AggregateValue[V])]
  type SummerOutput[K, V] = Item[(K, (Option[V], V))]

  type FlatMapBoltId[T] = Topology.BoltId[Item[T], _]
  type SummerBoltId[K, V] = Topology.BoltId[SummerInput[K, V], SummerOutput[K, V]]
}

/**
 * This class encapsulates logic how to build `StormTopology` from DAG of the job, jobId and options.
 */
case class StormTopologyBuilder(options: Map[String, Options], jobId: JobId, stormDag: Dag[Storm]) {
  import StormTopologyBuilder._

  def build: StormTopology = {
    val topology = stormDag.nodes.foldLeft(Topology.empty) {
      case (currentTopology, node: SummerNode[Storm]) =>
        SummerBoltProvider(this, node).register[Any, Any](currentTopology)
      case (currentTopology, node: FlatMapNode[Storm]) =>
        FlatMapBoltProvider(this, node).register(currentTopology)
      case (currentTopology, node: SourceNode[Storm]) =>
        SpoutProvider(this, node).register[Any](currentTopology)
    }
    topology.build(jobId)
  }

  private[storm] def registerAggregatedEdges[K, V](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[Aggregated[K, V]]
  ): Topology =
  // todo: validate all downstream nodes being with same semigroup and shards count
    stormDag.dependantsOf(source).foldLeft(topology) {
      // Downstream node must be `SummerNode`.
      case (currentTopology, downstreamNode: SummerNode[Storm]) =>
        currentTopology.withEdge(aggregatedToSummerEdge(sourceId, downstreamNode))
    }

  private[storm] def registerItemEdges[T](
    topology: Topology,
    source: StormNode,
    sourceId: Topology.EmittingId[Item[T]]
  ): Topology = {
    stormDag.dependantsOf(source).foldLeft(topology) {
      case (currentTopology, downstreamNode: FlatMapNode[Storm]) =>
        currentTopology.withEdge(itemToFlatMapEdge(sourceId, downstreamNode))
      case (currentTopology, downstreamNode: SummerNode[Storm]) =>
        val kvSourceId = sourceId.asInstanceOf[Topology.EmittingId[Item[(Any, Any)]]]
        currentTopology.withEdge(itemToSummerEdge[Any, Any](kvSourceId, downstreamNode))
    }
  }

  private def aggregatedToSummerEdge[K, V](
    sourceId: Topology.EmittingId[Aggregated[K, V]],
    node: SummerNode[Storm]
  ): Topology.Edge[Aggregated[K, V], SummerInput[K, V]] = Topology.Edge(
    sourceId,
    EdgeType.AggregatedKeyValues(getSummerKeyValueShards(node)),
    _._2,
    getSummerBoltId[K, V](node)
  )

  private def itemToFlatMapEdge[T](
    sourceId: Topology.EmittingId[Item[T]],
    node: FlatMapNode[Storm]
  ): Topology.Edge[Item[T], Item[T]] = {
    val usePreferLocalDependency = getOrElse(node, DEFAULT_FM_PREFER_LOCAL_DEPENDENCY)
    logger.info(s"[${getNodeName(node)}] usePreferLocalDependency: ${usePreferLocalDependency.get}")

    Topology.Edge[Item[T], Item[T]](
      sourceId,
      EdgeType.item(usePreferLocalDependency.get),
      identity,
      getFMBoltId[T](node)
    )
  }

  private def itemToSummerEdge[K, V](
    sourceId: Topology.EmittingId[Item[(K, V)]],
    node: SummerNode[Storm]
  ): Topology.Edge[Item[(K, V)], SummerInput[K, V]] = {
    throw new Exception("cannot emit to summer nodes from item source")
  }

  private[storm] def getSummerBoltId[K, V](node: SummerNode[Storm]): SummerBoltId[K, V] =
    Topology.BoltId(getNodeName(node))

  private[storm] def getFMBoltId[T](node: FlatMapNode[Storm]): FlatMapBoltId[T] =
    Topology.BoltId(getNodeName(node))

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

  private[storm] def getSummerKeyValueShards(summer: SummerNode[Storm]): executor.KeyValueShards = {
    // Query to get the summer paralellism of the summer down stream of us we are emitting to
    // to ensure no edge case between what we might see for its parallelism and what it would see/pass to storm.
    val summerParalellism = getOrElse(summer, DEFAULT_SUMMER_PARALLELISM)
    val summerBatchMultiplier = getOrElse(summer, DEFAULT_SUMMER_BATCH_MULTIPLIER)

    executor.KeyValueShards(summerParalellism.parHint * summerBatchMultiplier.get)
  }

  private[storm] def getNodeName(node: StormNode) = stormDag.getNodeName(node)
}
