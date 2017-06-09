package com.twitter.summingbird.storm

import com.twitter.summingbird.{ Options, Summer }
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.online.{ FlatMapOperation, MergeableStoreFactory, WrappedTSInMergeable, executor }
import com.twitter.summingbird.online.option.IncludeSuccessHandler
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, FlatMapNode, SourceNode, SummerNode }
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.builder.Topology.ReceivingId
import com.twitter.summingbird.storm.builder.{ EdgeType, Topology }
import com.twitter.summingbird.storm.option.AnchorTuples
import com.twitter.summingbird.storm.planner.StormNode
import org.apache.storm.generated.StormTopology
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

/**
 * This class encapsulates logic how to build `StormTopology` from DAG of the job, jobId and options.
 */
case class StormTopologyBuilder(options: Map[String, Options], jobId: JobId, stormDag: Dag[Storm]) {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormTopologyBuilder])

  def build: StormTopology = registerEdges(registerNodes).build(jobId)

  private def registerNodes: Topology =
    stormDag.nodes.foldLeft(Topology.EMPTY) {
      case (topology, node: SummerNode[Storm]) =>
        topology.withBolt(getNodeName(node), registerSummerBolt(node))._2
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

  private def registerSummerBolt(node: SummerNode[Storm]): Topology.Bolt[_, _] = {
    val nodeName = getNodeName(node)

    def sinkBolt[K, V](summer: Summer[Storm, K, V]): Topology.Bolt[_, _] = {
      implicit val semigroup = summer.semigroup
      implicit val batcher = summer.store.mergeableBatcher

      type ExecutorKeyType = (K, BatchID)
      type ExecutorValueType = (Timestamp, V)
      type ExecutorOutputType = (Timestamp, (K, (Option[V], V)))

      val anchorTuples = getOrElse(node, AnchorTuples.default)
      val metrics = getOrElse(node, DEFAULT_SUMMER_STORM_METRICS)
      val shouldEmit = stormDag.dependantsOf(node).size > 0

      val builder = BuildSummer(this, node)

      val ackOnEntry = getOrElse(node, DEFAULT_ACK_ON_ENTRY)
      logger.info(s"[$nodeName] ackOnEntry : ${ackOnEntry.get}")

      val maxEmitPerExecute = getOrElse(node, DEFAULT_MAX_EMIT_PER_EXECUTE)
      logger.info(s"[$nodeName] maxEmitPerExecute : ${maxEmitPerExecute.get}")

      val maxExecutePerSec = getOrElse(node, DEFAULT_MAX_EXECUTE_PER_SEC)
      logger.info(s"[$nodeName] maxExecutePerSec : $maxExecutePerSec")

      val parallelism = getOrElse(node, DEFAULT_SUMMER_PARALLELISM).parHint
      logger.info(s"[$nodeName] parallelism : $parallelism")

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
          getOrElse(node, DEFAULT_ONLINE_SUCCESS_HANDLER),
          getOrElse(node, DEFAULT_ONLINE_EXCEPTION_HANDLER),
          builder,
          getOrElse(node, DEFAULT_MAX_WAITING_FUTURES),
          getOrElse(node, DEFAULT_MAX_FUTURE_WAIT_TIME),
          maxEmitPerExecute,
          getOrElse(node, IncludeSuccessHandler.default))
      )
    }

    val summerUntyped = node.members.collect { case c@Summer(_, _, _) => c }.head
    sinkBolt(summerUntyped)
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

  private[storm] def getSummerKeyValueShards(summer: SummerNode[Storm]): executor.KeyValueShards = {
    // Query to get the summer paralellism of the summer down stream of us we are emitting to
    // to ensure no edge case between what we might see for its parallelism and what it would see/pass to storm.
    val summerParalellism = getOrElse(summer, DEFAULT_SUMMER_PARALLELISM)
    val summerBatchMultiplier = getOrElse(summer, DEFAULT_SUMMER_BATCH_MULTIPLIER)

    executor.KeyValueShards(summerParalellism.parHint * summerBatchMultiplier.get)
  }

  private[storm] def getNodeName(node: StormNode) = stormDag.getNodeName(node)
}
