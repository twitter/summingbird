package com.twitter.summingbird.storm

import com.twitter.summingbird.{ Options, Summer }
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.online.{ FlatMapOperation, MergeableStoreFactory, WrappedTSInMergeable, executor }
import com.twitter.summingbird.online.option.IncludeSuccessHandler
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, FlatMapNode, SourceNode, SummerNode }
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.option.AnchorTuples
import com.twitter.summingbird.storm.planner.StormNode
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.{ Config => BacktypeStormConfig }
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

case class StormTopologyBuilder(options: Map[String, Options], jobId: JobId, stormDag: Dag[Storm]) {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormTopologyBuilder])

  def build(): StormTopology = {
    val topologyBuilder = new TopologyBuilder
    stormDag.nodes.foreach {
      case summerNode: SummerNode[Storm] => scheduleSummerBolt(summerNode, topologyBuilder)
      case flatMapNode: FlatMapNode[Storm] => scheduleFlatMapper(flatMapNode, topologyBuilder)
      case sourceNode: SourceNode[Storm] => scheduleSpout(sourceNode, topologyBuilder)
    }
    topologyBuilder.createTopology()
  }

  /**
   * Set storm to tick our nodes every second to clean up finished futures
   */
  private def tickConfig = {
    val boltConfig = new BacktypeStormConfig
    boltConfig.put(BacktypeStormConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS, java.lang.Integer.valueOf(1))
    boltConfig
  }

  private def scheduleFlatMapper(node: FlatMapNode[Storm], topologyBuilder: TopologyBuilder) = {
    val bolt: BaseBolt[Any, Any] = FlatMapBoltProvider(this, node).apply

    val parallelism = getOrElse(node, DEFAULT_FM_PARALLELISM).parHint
    val declarer = topologyBuilder.setBolt(getNodeName(node), bolt, parallelism).addConfigurations(tickConfig)
    bolt.applyGroupings(declarer)
  }

  private def scheduleSpout(node: SourceNode[Storm], topologyBuilder: TopologyBuilder) = {
    val nodeName = stormDag.getNodeName(node)
    val (sourceParalleism, stormSpout) = SpoutProvider(this, node).apply
    topologyBuilder.setSpout(nodeName, stormSpout, sourceParalleism)
  }

  private def scheduleSummerBolt(node: SummerNode[Storm], topologyBuilder: TopologyBuilder): Unit = {

    val nodeName = getNodeName(node)

    def sinkBolt[K, V](summer: Summer[Storm, K, V]): BaseBolt[_, _] = {
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

      val storeBaseFMOp = { op: (ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)) =>
        val ((k, batchID), (optiVWithTS, (ts, v))) = op
        val optiV = optiVWithTS.map(_._2)
        List((ts, (k, (optiV, v))))
      }

      val flatmapOp: FlatMapOperation[(ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)), ExecutorOutputType] =
        FlatMapOperation.apply(storeBaseFMOp)

      val supplier: MergeableStoreFactory[ExecutorKeyType, V] = summer.store

      val dependenciesNames = stormDag.dependenciesOf(node).collect { case x: StormNode => getNodeName(x) }
      val inputEdges = dependenciesNames.map(parent =>
        (parent, EdgeType.AggregatedKeyValues[ExecutorKeyType, ExecutorValueType](getSummerKeyValueShards(node)))
      ).toMap

      BaseBolt(
        jobId,
        metrics.metrics,
        anchorTuples,
        shouldEmit,
        ackOnEntry,
        maxExecutePerSec,
        inputEdges,
        // Output edge's grouping isn't important for now.
        EdgeType.itemWithLocalOrShuffleGrouping[ExecutorOutputType],
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
    val parallelism = getOrElse(node, DEFAULT_SUMMER_PARALLELISM).parHint
    val bolt = sinkBolt(summerUntyped)
    val declarer =
      topologyBuilder.setBolt(
        nodeName,
        bolt,
        parallelism
      ).addConfigurations(tickConfig)
    bolt.applyGroupings(declarer)
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
