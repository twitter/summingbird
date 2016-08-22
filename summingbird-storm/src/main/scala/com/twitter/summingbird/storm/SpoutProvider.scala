package com.twitter.summingbird.storm

import backtype.storm.metric.api.IMetric
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{ IRichSpout, TopologyBuilder }
import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.online.{ Externalizer, executor }
import com.twitter.summingbird.online.option.SummerBuilder
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, SummerNode }
import com.twitter.summingbird.storm.planner.StormNode
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird.storm.Constants
import com.twitter.summingbird.storm.option.SpoutStormMetrics
import com.twitter.summingbird.storm.spout.KeyValueSpout

case class SpoutProvider(storm: Storm, stormDag: Dag[Storm], node: StormNode, jobID: JobId)(implicit topologyBuilder: TopologyBuilder) {

  private def getOrElse[T <: AnyRef: Manifest](stormDag: Dag[Storm], queryNode: StormNode = node, default: T) = storm.getOrElse(stormDag, queryNode, default)

  private val metrics = getOrElse(stormDag, node, Constants.DEFAULT_SPOUT_STORM_METRICS)

  private def computerSpout(): (Int, Spout[(Timestamp, Any)]) = {
    val (spout, parOpt) = node.members.collect { case Source(SpoutSource(s, parOpt)) => (s, parOpt) }.head
    val nodeName = stormDag.getNodeName(node)
    val sourceParallelism = getOrElse(stormDag, node, parOpt.getOrElse(Constants.DEFAULT_SOURCE_PARALLELISM)).parHint
    val computedSpout = node.members.reverse.foldLeft(spout.asInstanceOf[Spout[(Timestamp, Any)]]) { (spout, p) =>
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
    (sourceParallelism, computedSpout)
  }

  // Tormenta bug when openHook might get lost if it doesn't happen right before the getSpout.
  private def getStormSpout[T](spout: Spout[T], register: Externalizer[(TopologyContext) => Unit]): IRichSpout = {
    val hookedSpout = spout.openHook(register.get)
    hookedSpout.getSpout
  }

  /**
   * This method checks that sourceParallelism is greater than flatMapParallelism when FMMergeableWithSource is opted-in
   */
  private def checkParallelisms(stormDag: Dag[Storm], node: StormNode, sourceParallelism: Int): Unit = {
    val isMergeableWithSource = getOrElse(stormDag, node, Constants.DEFAULT_FM_MERGEABLE_WITH_SOURCE).get
    if (isMergeableWithSource) {
      val flatMapParallelism = getOrElse(stormDag, node, Constants.DEFAULT_FM_PARALLELISM).parHint
      require(flatMapParallelism <= sourceParallelism, s"SourceParallelism ($sourceParallelism) must be at least as high as FlatMapParallelism ($flatMapParallelism) when merging flatMap with Source")
    }
  }

  private def getCountersForJob(jobId: JobId): Seq[(Group, Name)] = JobCounters.getCountersForJob(jobId).getOrElse(Nil)

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

  private def counter(jobID: JobId, nodeName: Group, counterName: Name) =
    { new Counter(Group("summingbird." + nodeName.getString), counterName)(jobID) with Incrementor }

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
      case (time, (k: K, v: V)) => ((k, batcher.batchOf(time)), (time, v))
    }
    implicit val valueMonoid: Semigroup[V] = summerProducer.semigroup
    new KeyValueSpout[(K, BatchID), (Timestamp, V)](getStormSpout(formattedSummerSpout, fn), builder, keyValueShards, flushExecTimeCounter, executeTimeCounter)
  }

  /**
   * This method creates the counters expicit for the KeyValueSpout.
   * The function to register all the counters is created with all the registered metrics.
   */
  private def getKeyValueSpout[K, V](
    tormentaSpout: Spout[(Timestamp, (K, V))],
    sNode: StormNode): IRichSpout = {

    val builder = BuildSummer(storm, stormDag, node, jobID)
    val nodeName = stormDag.getNodeName(node)
    val flushExecTimeCounter = counter(jobID, Group(nodeName), Name("spoutFlushExecTime"))
    val executeTimeCounter = counter(jobID, Group(nodeName), Name("spoutEmitExecTime"))
    val registerAllMetricsAndCounters = createRegistrationFunction(metrics, getCountersForJob(jobID), jobID)
    createSpoutToFeedSummer[K, V](stormDag, sNode, builder, tormentaSpout, registerAllMetricsAndCounters, flushExecTimeCounter, executeTimeCounter)
  }

  /**
   * This method registers the common counters and gets the stormSpout.
   */
  private def getSpout(tormentaSpout: Spout[(Timestamp, Any)]): IRichSpout = {
    val registerAllMetricsAndCounters = createRegistrationFunction(metrics, getCountersForJob(jobID), jobID)
    getStormSpout(tormentaSpout, registerAllMetricsAndCounters)
  }

  /**
   * This is the decision logic.
   * If the spout is followed by a summer wraps the spout in KeyValueSpout
   */
  def apply: (Int, IRichSpout) = {
    val (sourceParallelism, tormentaSpout) = computerSpout()
    val summerOpt: Option[SummerNode[Storm]] = stormDag.dependantsOf(node.asInstanceOf[StormNode]).collect { case s: SummerNode[Storm] => s }.headOption
    val spout: IRichSpout = summerOpt match {
      case Some(summerNode) => getKeyValueSpout[Any, Any](tormentaSpout.asInstanceOf[Spout[(Timestamp, (Any, Any))]], summerNode)
      case None => getSpout(tormentaSpout)
    }
    (sourceParallelism, spout)
  }
}
