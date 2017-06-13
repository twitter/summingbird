package com.twitter.summingbird.storm

import com.twitter.summingbird.Options
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, FlatMapNode, SourceNode, SummerNode }
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.builder.Topology.ReceivingId
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

  type ItemSpout[T] = Topology.Spout[Item[T]]
  type AggregatedSpout[K, V] = Topology.Spout[Aggregated[K, V]]

  type ItemFMBolt[T, U] = Topology.Bolt[Item[T], Item[U]]
  type AggregatedFMBolt[T, K, V] = Topology.Bolt[Item[T], Aggregated[K, V]]

  type SummerOutput[K, V] = Item[(K, (Option[V], V))]
  type SummerBolt[K, V] = Topology.Bolt[Aggregated[K, V], SummerOutput[K, V]]
}

/**
 * This class encapsulates logic how to build `StormTopology` from DAG of the job, jobId and options.
 */
case class StormTopologyBuilder(options: Map[String, Options], jobId: JobId, stormDag: Dag[Storm]) {
  import StormTopologyBuilder._

  def build: StormTopology = registerEdges(registerNodes).build(jobId)

  private def registerNodes: Topology =
    stormDag.nodes.foldLeft(Topology.empty) {
      case (topology, node: SummerNode[Storm]) =>
        topology.withBolt(getNodeName(node), SummerBoltProvider(this, node).apply)._2
      case (topology, node: FlatMapNode[Storm]) =>
        topology.withBolt(getNodeName(node), FlatMapBoltProvider(this, node).apply)._2
      case (topology, node: SourceNode[Storm]) =>
        topology.withSpout(getNodeName(node), SpoutProvider(this, node).apply)._2
    }

  private def registerEdges(topologyWithNodes: Topology): Topology =
    stormDag.nodes.foldLeft(topologyWithNodes) {
      case (topology, node: SummerNode[Storm]) =>
        val edgeType = EdgeType.AggregatedKeyValues[Any, Any](getSummerKeyValueShards(node))
        registerIncomingEdges(topology, node, edgeType)
      case (topology, node: FlatMapNode[Storm]) =>
        registerIncomingEdges(topology, node, flatMapEdgeType(node))
      case (topology, node: SourceNode[Storm]) => topology
        // ignore, no incoming edges into sources
    }

  private def flatMapEdgeType(node: FlatMapNode[Storm]): EdgeType[_] = {
    val usePreferLocalDependency = getOrElse(node, DEFAULT_FM_PREFER_LOCAL_DEPENDENCY)
    logger.info(s"[${getNodeName(node)}] usePreferLocalDependency: ${usePreferLocalDependency.get}")
    if (usePreferLocalDependency.get) {
      EdgeType.itemWithLocalOrShuffleGrouping[Any]
    } else {
      EdgeType.itemWithShuffleGrouping[Any]
    }
  }

  private def registerIncomingEdges(topology: Topology, node: StormNode, edgeType: EdgeType[_]): Topology = {
    val nodeId: ReceivingId[Any] = getId(node).asInstanceOf[Topology.ReceivingId[Any]]
    stormDag.dependenciesOf(node).foldLeft(topology) { (current, upstreamNode) =>
      current.withEdge(Topology.Edge[Any](
        getId(upstreamNode),
        edgeType.asInstanceOf[EdgeType[Any]],
        nodeId))
    }
  }

  private def getId(node: StormNode): Topology.EmittingId[_] = node match {
    case sourceNode: SourceNode[Storm] => getSourceId(sourceNode)
    case flatMapNode: FlatMapNode[Storm] => getFlatMapId(flatMapNode)
    case summerNode: SummerNode[Storm] => getSummerId(summerNode)
  }

  private def getSummerId(node: SummerNode[Storm]): Topology.BoltId[_, _] =
    Topology.BoltId(getNodeName(node))

  private def getFlatMapId(node: FlatMapNode[Storm]): Topology.BoltId[_, _] =
    Topology.BoltId(getNodeName(node))

  private def getSourceId(node: SourceNode[Storm]): Topology.SpoutId[_] =
    Topology.SpoutId(getNodeName(node))

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
