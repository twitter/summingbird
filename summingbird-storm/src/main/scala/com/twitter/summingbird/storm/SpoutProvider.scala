package com.twitter.summingbird.storm

import backtype.storm.metric.api.IMetric
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{ IRichSpout, TopologyBuilder }
import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.online.{ Externalizer, executor }
import com.twitter.summingbird.online.option.{ SourceParallelism, SummerBuilder }
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, SummerNode }
import com.twitter.summingbird.storm.planner.StormNode
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird.storm.option.SpoutStormMetrics
import com.twitter.summingbird.storm.spout.KeyValueSpout

case class SpoutProvider(storm: Storm, stormDag: Dag[Storm], node: StormNode, jobID: JobId)(implicit topologyBuilder: TopologyBuilder) {

  private def getOrElse[T <: AnyRef: Manifest](node: StormNode, default: T): T = storm.getOrElse(stormDag, node, default)

  private def getSourceParallelism(sourceParOption: Option[SourceParallelism]) =
    getOrElse(node, sourceParOption.getOrElse(Constants.DEFAULT_SOURCE_PARALLELISM)).parHint

  private val spoutStormMetrics = getOrElse(node, Constants.DEFAULT_SPOUT_STORM_METRICS)

  private def extractSourceAttributes: (Spout[(Timestamp, Any)], Option[SourceParallelism]) =
    node.members.collect { case Source(SpoutSource(s, parOpt)) => (s, parOpt) }.head

  private def computeSpout(spout: Spout[(Timestamp, Any)]): Spout[(Timestamp, Any)] = {
    val nodeName = stormDag.getNodeName(node)
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
    }
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
    summerNode: SummerNode[Storm],
    builder: SummerBuilder,
    spout: Spout[(Timestamp, (K, V))],
    flushExecTimeCounter: Incrementor,
    executeTimeCounter: Incrementor): IRichSpout = {

    val summerParalellism = getOrElse(summerNode, Constants.DEFAULT_SUMMER_PARALLELISM)
    val summerBatchMultiplier = getOrElse(summerNode, Constants.DEFAULT_SUMMER_BATCH_MULTIPLIER)
    val keyValueShards = executor.KeyValueShards(summerParalellism.parHint * summerBatchMultiplier.get)
    val summerProducer = summerNode.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Storm, K, V]]
    val batcher = summerProducer.store.mergeableBatcher
    val formattedSummerSpout = spout.map {
      case (time, (k, v)) => ((k, batcher.batchOf(time)), (time, v))
    }
    implicit val valueMonoid: Semigroup[V] = summerProducer.semigroup
    val stormSpout = getStormSpout(formattedSummerSpout)

    val maxEmitPerExecute = getOrElse(node, Constants.DEFAULT_MAX_EMIT_PER_EXECUTE)

    new KeyValueSpout[(K, BatchID), (Timestamp, V)](
      stormSpout,
      builder,
      maxEmitPerExecute,
      keyValueShards,
      flushExecTimeCounter,
      executeTimeCounter
    )
  }

  private def getCountersForJob: Seq[(Group, Name)] = JobCounters.getCountersForJob(jobID).getOrElse(Nil)

  private def counter(nodeName: Group, counterName: Name): Counter with Incrementor =
    new Counter(Group("summingbird." + nodeName.getString), counterName)(jobID) with Incrementor

  // Tormenta bug when openHook might get lost if it doesn't happen right before the getSpout.
  private def getStormSpout[T](spout: Spout[T]): IRichSpout = {
    val register: Externalizer[(TopologyContext) => Unit] = createRegistrationFunction
    spout.openHook(register.get).getSpout
  }

  /**
   * This method creates the counters expicit for the KeyValueSpout.
   * The function to register all the counters is created with all the registered metrics.
   */
  private def getKeyValueSpout[K, V](
    tormentaSpout: Spout[(Timestamp, (K, V))],
    sNode: SummerNode[Storm]): IRichSpout = {
    val builder = BuildSummer(storm, stormDag, node, jobID)
    val nodeName = stormDag.getNodeName(node)
    val flushExecTimeCounter = counter(Group(nodeName), Name("spoutFlushExecTime"))
    val executeTimeCounter = counter(Group(nodeName), Name("spoutEmitExecTime"))
    createSpoutToFeedSummer[K, V](sNode, builder, tormentaSpout, flushExecTimeCounter, executeTimeCounter)
  }

  /**
   * This function takes the metrics and counters to register to the TopologyContext of a JobID and outputs a function which can be called on openHook.
   */
  private def createRegistrationFunction: Externalizer[(TopologyContext) => Unit] = {
    val counters: Seq[(Group, Name)] = getCountersForJob
    Externalizer({ context: TopologyContext =>
      // Register metrics passed in SpoutStormMetrics option.
      spoutStormMetrics.metrics().foreach {
        x: StormMetric[IMetric] =>
          context.registerMetric(x.name, x.metric, x.interval.inSeconds)
      }
      // Register summingbird counter metrics.
      StormStatProvider.registerMetrics(jobID, context, counters)
      SummingbirdRuntimeStats.addPlatformStatProvider(StormStatProvider)
    })
  }

  /**
   * This is the decision logic.
   * If the spout is followed by a summer wraps the spout in KeyValueSpout
   */
  def apply: (Int, IRichSpout) = {
    val (spout, sourceParallelismOption) = extractSourceAttributes
    val tormentaSpout = computeSpout(spout)
    val sourceParallelism = getSourceParallelism(sourceParallelismOption)
    validateParallelisms(sourceParallelism)
    val summerOpt: Option[SummerNode[Storm]] = stormDag.dependantsOf(node.asInstanceOf[StormNode]).collect { case s: SummerNode[Storm] => s }.headOption
    val stormSpout: IRichSpout = summerOpt match {
      case Some(summerNode) => {
        // since we know that the the next node is a summer, this implies the type of the spout must be `(K, V)` for some `K, V`
        val kvSpout = tormentaSpout.asInstanceOf[Spout[(Timestamp, (Any, Any))]]
        getKeyValueSpout(kvSpout, summerNode)
      }
      case None => getStormSpout(tormentaSpout)
    }
    (sourceParallelism, stormSpout)
  }
}
