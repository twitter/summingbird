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
import com.twitter.summingbird.storm.Constants
import com.twitter.summingbird.storm.option.SpoutStormMetrics
import com.twitter.summingbird.storm.spout.KeyValueSpout

case class SpoutProvider(storm: Storm, stormDag: Dag[Storm], node: StormNode, jobID: JobId)(implicit topologyBuilder: TopologyBuilder) {

  private def getOrElse[T <: AnyRef: Manifest](stormDag: Dag[Storm], queryNode: StormNode = node, default: T): T = storm.getOrElse(stormDag, queryNode, default)

  private def getSourceParallelism(sourceParOption: Option[SourceParallelism]) =
    getOrElse(stormDag, node, sourceParOption.getOrElse(Constants.DEFAULT_SOURCE_PARALLELISM)).parHint

  private val spoutStormMetrics = getOrElse(stormDag, node, Constants.DEFAULT_SPOUT_STORM_METRICS)

  private def extractSourceAttributes: (Spout[(Timestamp, Any)], Option[SourceParallelism]) = {
    node.members.collect { case Source(SpoutSource(s, parOpt)) => (s, parOpt) }.head
  }

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
  private def validateParallelisms(stormDag: Dag[Storm], node: StormNode, sourceParallelism: Int): Unit = {
    val isMergeableWithSource = getOrElse(stormDag, node, Constants.DEFAULT_FM_MERGEABLE_WITH_SOURCE).get
    if (isMergeableWithSource) {
      val flatMapParallelism = getOrElse(stormDag, node, Constants.DEFAULT_FM_PARALLELISM).parHint
      require(flatMapParallelism <= sourceParallelism, s"SourceParallelism ($sourceParallelism) must be at least as high as FlatMapParallelism ($flatMapParallelism) when merging flatMap with Source")
    }
  }

  /*
   * This method formats the tuples to batched format and wraps the spout into KeyValueSpout.
   */
  private def createSpoutToFeedSummer[K, V](
    stormDag: Dag[Storm],
    node: StormNode,
    builder: SummerBuilder,
    spout: Spout[(Timestamp, (K, V))],
    fn: Externalizer[(TopologyContext) => Unit],
    flushExecTimeCounter: Incrementor,
    executeTimeCounter: Incrementor): IRichSpout = {

    val summerParalellism = getOrElse(stormDag, node, Constants.DEFAULT_SUMMER_PARALLELISM)
    val summerBatchMultiplier = getOrElse(stormDag, node, Constants.DEFAULT_SUMMER_BATCH_MULTIPLIER)
    val keyValueShards = executor.KeyValueShards(summerParalellism.parHint * summerBatchMultiplier.get)
    val summerProducer = node.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Storm, K, V]]
    val batcher = summerProducer.store.mergeableBatcher
    val formattedSummerSpout = spout.map {
      case (time, (k, v)) => ((k, batcher.batchOf(time)), (time, v))
    }
    implicit val valueMonoid: Semigroup[V] = summerProducer.semigroup
    new KeyValueSpout[(K, BatchID), (Timestamp, V)](getStormSpout(formattedSummerSpout, fn), builder, keyValueShards, flushExecTimeCounter, executeTimeCounter)
  }

  private def getCountersForJob(jobId: JobId): Seq[(Group, Name)] = JobCounters.getCountersForJob(jobId).getOrElse(Nil)

  private def counter(jobID: JobId, nodeName: Group, counterName: Name) =
    { new Counter(Group("summingbird." + nodeName.getString), counterName)(jobID) with Incrementor }

  // Tormenta bug when openHook might get lost if it doesn't happen right before the getSpout.
  private def getStormSpout[T](spout: Spout[T], register: Externalizer[(TopologyContext) => Unit]): IRichSpout = {
    spout.openHook(register.get).getSpout
  }

  /**
   * This method creates the counters expicit for the KeyValueSpout.
   * The function to register all the counters is created with all the registered metrics.
   */
  private def getKeyValueSpout[K, V](
    tormentaSpout: Spout[(Timestamp, (K, V))],
    sNode: StormNode): IRichSpout = {

    val builder = BuildSummerSpout(storm, stormDag, node, jobID)
    val nodeName = stormDag.getNodeName(node)
    val flushExecTimeCounter = counter(jobID, Group(nodeName), Name("spoutFlushExecTime"))
    val executeTimeCounter = counter(jobID, Group(nodeName), Name("spoutEmitExecTime"))
    val registerAllMetricsAndCounters = createRegistrationFunction(spoutStormMetrics, getCountersForJob(jobID), jobID)
    createSpoutToFeedSummer[K, V](stormDag, sNode, builder, tormentaSpout, registerAllMetricsAndCounters, flushExecTimeCounter, executeTimeCounter)
  }

  /**
   * This function takes the metrics and counters to register to the TopologyContext of a JobID and outputs a function which can be called on openHook.
   */
  private def createRegistrationFunction(
    metrics: SpoutStormMetrics,
    counters: Seq[(Group, Name)],
    jobID: JobId): Externalizer[(TopologyContext) => Unit] = {

    Externalizer({ context: TopologyContext =>
      // Register metrics passed in SpoutStormMetrics option.
      metrics.metrics().foreach {
        x: StormMetric[IMetric] =>
          context.registerMetric(x.name, x.metric, x.interval.inSeconds)
      }
      // Register summingbird counter metrics.
      StormStatProvider.registerMetrics(jobID, context, counters)
      SummingbirdRuntimeStats.addPlatformStatProvider(StormStatProvider)
    })
  }

  /**
   * This method registers the common counters and gets the stormSpout.
   */
  private def getSpout(tormentaSpout: Spout[(Timestamp, Any)]): IRichSpout = {
    val registerAllMetricsAndCounters = createRegistrationFunction(spoutStormMetrics, getCountersForJob(jobID), jobID)
    getStormSpout(tormentaSpout, registerAllMetricsAndCounters)
  }

  /**
   * This is the decision logic.
   * If the spout is followed by a summer wraps the spout in KeyValueSpout
   */
  def apply: (Int, IRichSpout) = {
    val (spout, sourceParallelismOption) = extractSourceAttributes
    val tormentaSpout = computeSpout(spout)
    val sourceParallelism = getSourceParallelism(sourceParallelismOption)
    validateParallelisms(stormDag, node, sourceParallelism)
    val summerOpt: Option[SummerNode[Storm]] = stormDag.dependantsOf(node.asInstanceOf[StormNode]).collect { case s: SummerNode[Storm] => s }.headOption
    val stormSpout: IRichSpout = summerOpt match {
      case Some(summerNode) => {
        // since we know that the the next node is a summer, this implies the type of the spout must be `(K, V)` for some `K, V`
        val kvSpout = tormentaSpout.asInstanceOf[Spout[(Timestamp, (Any, Any))]]
        getKeyValueSpout(kvSpout, summerNode)
      }
      case None => getSpout(tormentaSpout)
    }
    (sourceParallelism, stormSpout)
  }
}
