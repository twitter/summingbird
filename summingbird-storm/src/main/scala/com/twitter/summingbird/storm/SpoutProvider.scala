package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird._
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.online.option.{ SourceParallelism, SummerBuilder }
import com.twitter.summingbird.planner.{ SourceNode, SummerNode }
import com.twitter.summingbird.storm.StormTopologyBuilder._
import com.twitter.summingbird.storm.builder.Topology
import com.twitter.summingbird.storm.planner.StormNode
import com.twitter.tormenta.spout.Spout
import scala.reflect.ClassTag

case class SpoutProvider(builder: StormTopologyBuilder, node: SourceNode[Storm]) {

  private def getOrElse[T <: AnyRef: ClassTag](node: StormNode, default: T): T =
    builder.getOrElse(node, default)

  private def getSourceParallelism(sourceParOption: Option[SourceParallelism]) =
    getOrElse(node, sourceParOption.getOrElse(Constants.DEFAULT_SOURCE_PARALLELISM)).parHint

  private val spoutStormMetrics = getOrElse(node, Constants.DEFAULT_SPOUT_STORM_METRICS)

  private def extractSourceAttributes: (Spout[(Timestamp, Any)], Option[SourceParallelism]) =
    node.members.collect { case Source(SpoutSource(s, parOpt)) => (s, parOpt) }.head

  private def computeSpout[T](spout: Spout[(Timestamp, _)]): Spout[Item[T]] = {
    node.members.reverse.foldLeft(spout.asInstanceOf[Spout[(Timestamp, Any)]]) { (spout, p) =>
      p match {
        case Source(_) => spout // The source is still in the members list so drop it
        case OptionMappedProducer(_, op) =>
          val boxed = Externalizer(op)
          spout.flatMap { case (time, t) => boxed.get(t).map { x => (time, x) } }
        case FlatMappedProducer(_, op) => {
          val lockedOp = Externalizer(op)
          spout.flatMap { case (time, t) => lockedOp.get.apply(t).map { x => (time, x) } }
        }
        case NamedProducer(_, _) => spout
        case IdentityKeyedProducer(_) => spout
        case AlsoProducer(_, _) => spout
        case _ => sys.error("not possible, given the above call to span.\n" + p)
      }
    }.asInstanceOf[Spout[Item[T]]]
  }

  /**
   * This method checks that sourceParallelism is greater than flatMapParallelism when FMMergeableWithSource is opted-in
   */
  private def validateParallelisms(sourceParallelism: Int): Unit = {
    val isMergeableWithSource = getOrElse(node, Constants.DEFAULT_FM_MERGEABLE_WITH_SOURCE).get
    if (isMergeableWithSource) {
      val flatMapParallelism = getOrElse(node, Constants.DEFAULT_FM_PARALLELISM).parHint
      require(flatMapParallelism <= sourceParallelism,
        s"SourceParallelism ($sourceParallelism) must be at least as high as FlatMapParallelism ($flatMapParallelism) when merging flatMap with Source")
    }
  }

  /*
   * This method formats the tuples to batched format and wraps the spout into KeyValueSpout.
   */
  private def createSpoutToFeedSummer[K, V](
    sourceParallelism: Int,
    summerNode: SummerNode[Storm],
    builder: SummerBuilder,
    spout: Spout[Item[(K, V)]],
    flushExecTimeCounter: Incrementor,
    executeTimeCounter: Incrementor
  ): Topology.Spout[Aggregated[K, V]] = {
    val summerProducer = summerNode.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Storm, K, V]]
    val batcher = summerProducer.store.mergeableBatcher
    val formattedSummerSpout = spout.map {
      case (time, (k, v)) => ((k, batcher.batchOf(time)), (time, v))
    }
    implicit val valueMonoid: Semigroup[V] = summerProducer.semigroup

    val maxEmitPerExecute = getOrElse(node, Constants.DEFAULT_MAX_EMIT_PER_EXECUTE)

    Topology.KeyValueSpout(
      sourceParallelism,
      spoutStormMetrics.metrics,
      formattedSummerSpout,
      builder,
      maxEmitPerExecute,
      flushExecTimeCounter,
      executeTimeCounter
    )
  }

  private def counter(nodeName: Group, counterName: Name): Counter with Incrementor =
    new Counter(Group("summingbird." + nodeName.getString), counterName)(builder.jobId) with Incrementor

  /**
   * This method creates the counters expicit for the KeyValueSpout.
   * The function to register all the counters is created with all the registered metrics.
   */
  private def registerKeyValueSpout[K, V](
    topology: Topology,
    sourceParallelism: Int,
    tormentaSpout: Spout[Item[(K, V)]],
    sNode: SummerNode[Storm]
  ): Topology = {
    val summerBuilder = BuildSummer(builder, node)
    val nodeName = builder.getNodeName(node)
    val flushExecTimeCounter = counter(Group(nodeName), Name("spoutFlushExecTime"))
    val executeTimeCounter = counter(Group(nodeName), Name("spoutEmitExecTime"))
    val spout = createSpoutToFeedSummer[K, V](
      sourceParallelism, sNode, summerBuilder, tormentaSpout, flushExecTimeCounter, executeTimeCounter)
    val (spoutId, topologyWithSpout) = topology.withSpout(builder.getNodeName(node), spout)
    builder.registerAggregatedEdges[K, V](topologyWithSpout, node, spoutId)
  }

  private def registerRawSpout[T](
    topology: Topology,
    sourceParallelism: Int,
    tormentaSpout: Spout[Item[T]]
  ): Topology = {
    val bolt = Topology.RawSpout[Item[T]](sourceParallelism, spoutStormMetrics.metrics, tormentaSpout)
    val (boltId, topologyWithBolt) = topology.withSpout(builder.getNodeName(node), bolt)
    builder.registerItemEdges[T](topologyWithBolt, node, boltId)
  }

  /**
   * This is the decision logic.
   * If the spout is followed by a summer wraps the spout in KeyValueSpout
   */
  def register[T](topology: Topology): Topology = {
    val (spout, sourceParallelismOption) = extractSourceAttributes
    val tormentaSpout: Spout[Item[T]] = computeSpout(spout)
    val sourceParallelism = getSourceParallelism(sourceParallelismOption)
    validateParallelisms(sourceParallelism)
    val summerOpt: Option[SummerNode[Storm]] = builder.stormDag
      .dependantsOf(node.asInstanceOf[StormNode]).collect { case s: SummerNode[Storm] => s }
      .headOption
    summerOpt match {
      case Some(summerNode) =>
        // since we know that the the next node is a summer, this implies the type of the spout must be `(K, V)` for some `K, V`
        registerKeyValueSpout[Any, Any](
          topology,
          sourceParallelism,
          tormentaSpout.asInstanceOf[Spout[Item[(Any, Any)]]],
          summerNode
        )
      case None => registerRawSpout[T](topology, sourceParallelism, tormentaSpout)
    }
  }
}
