package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ Batcher, Timestamp }
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.SourceParallelism
import com.twitter.summingbird.planner.SourceNode
import com.twitter.summingbird.storm.StormTopologyBuilder._
import com.twitter.summingbird.storm.builder.Topology
import com.twitter.summingbird.storm.planner.StormNode
import com.twitter.tormenta.spout.Spout
import scala.reflect.ClassTag

private[storm] case class SpoutProvider(
  builder: StormTopologyBuilder,
  node: SourceNode[Storm]
) extends ComponentProvider {

  override def createSingle[T, O](fn: Item[T] => O): Topology.Component[O] = {
    val (tormentaSpout, parallelism) = getTormentaSpoutAndParallelism[T]
    Topology.RawSpout[O](parallelism, spoutStormMetrics.metrics, tormentaSpout.map(fn))
  }

  override def createAggregated[K, V](
    batcher: Batcher,
    shards: KeyValueShards,
    semigroup: Semigroup[V]
  ): Option[Topology.Component[Aggregated[K, V]]] = {
    val (tormentaSpout, parallelism) = getTormentaSpoutAndParallelism[(K, V)]
    val nodeName = builder.getNodeName(node)
    val summerBuilder = BuildSummer(builder, nodeName, node.lastProducer.get)
    val flushExecTimeCounter = counter(Group(nodeName), Name("spoutFlushExecTime"))
    val executeTimeCounter = counter(Group(nodeName), Name("spoutEmitExecTime"))

    val formattedSummerSpout = tormentaSpout.map {
      case (time, (k, v)) => ((k, batcher.batchOf(time)), (time, v))
    }
    implicit val valueMonoid: Semigroup[V] = semigroup

    val maxEmitPerExecute = getOrElse(node, Constants.DEFAULT_MAX_EMIT_PER_EXECUTE)

    Some(Topology.KeyValueSpout(
      parallelism,
      spoutStormMetrics.metrics,
      formattedSummerSpout,
      shards,
      summerBuilder,
      maxEmitPerExecute,
      flushExecTimeCounter,
      executeTimeCounter
    ))
  }

  private def getTormentaSpoutAndParallelism[T]: (Spout[Item[T]], Int) = {
    val (spout, sourceParallelismOption) = extractSourceAttributes
    val tormentaSpout: Spout[Item[T]] = computeSpout(spout)
    val sourceParallelism = getSourceParallelism(sourceParallelismOption)
    validateParallelisms(sourceParallelism)
    (tormentaSpout, sourceParallelism)
  }

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

  private def counter(nodeName: Group, counterName: Name): Counter with Incrementor =
    new Counter(Group("summingbird." + nodeName.getString), counterName)(builder.jobId) with Incrementor
}
