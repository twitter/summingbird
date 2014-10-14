/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.summingbird.storm

import backtype.storm.{ Config => BacktypeStormConfig, LocalCluster, StormSubmitter }
import backtype.storm.generated.StormTopology
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{ BoltDeclarer, TopologyBuilder }
import backtype.storm.tuple.Fields

import com.twitter.algebird.{ Monoid, Semigroup }
import com.twitter.bijection.{ Base64String, Injection }
import com.twitter.chill.IKryoRegistrar
import com.twitter.storehaus.algebra.{ MergeableStore, Mergeable, StoreAlgebra }
import com.twitter.storehaus.{ ReadableStore, WritableStore, Store }
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.chill.SBChillRegistrar
import com.twitter.summingbird.online._
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.{ Dag, DagOptimizer, OnlinePlan, SummerNode, FlatMapNode, SourceNode }
import com.twitter.summingbird.storm.option.{ AckOnEntry, AnchorTuples }
import com.twitter.summingbird.storm.planner.StormNode
import com.twitter.summingbird.viz.VizGraph
import com.twitter.tormenta.spout.Spout
import com.twitter.util.{ Future, Time }

import org.slf4j.LoggerFactory

import scala.collection.{ Map => CMap }

import Constants._

/*
 * Batchers are used for partial aggregation. We never aggregate past two items which are not in the same batch.
 * This is needed/used everywhere we partially aggregate, summer's into stores, map side partial aggregation before summers, etc..
 */

sealed trait StormSource[+T]

case class SpoutSource[+T](spout: Spout[(Timestamp, T)], parallelism: Option[SourceParallelism]) extends StormSource[T]

object Storm {
  def local(options: Map[String, Options] = Map.empty): LocalStorm =
    new LocalStorm(options, identity, List())

  def remote(options: Map[String, Options] = Map.empty): RemoteStorm =
    new RemoteStorm(options, identity, List())

  /**
   * Below are factory methods for the input output types:
   */

  def sink[T](fn: => T => Future[Unit]): Storm#Sink[T] = new SinkFn(fn)

  def sinkIntoWritable[K, V](store: => WritableStore[K, V]): Storm#Sink[(K, V)] =
    new WritableStoreSink[K, V](store)

  // This can be used in jobs that do not have a batch component
  def onlineOnlyStore[K, V](store: => MergeableStore[K, V]): MergeableStoreFactory[(K, BatchID), V] =
    MergeableStoreFactory.fromOnlineOnly(store)

  def store[K, V](store: => Mergeable[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreFactory[(K, BatchID), V] =
    MergeableStoreFactory.from(store)

  def service[K, V](serv: => ReadableStore[K, V]): ReadableServiceFactory[K, V] = ReadableServiceFactory(() => serv)

  def toStormSource[T](spout: Spout[T],
    defaultSourcePar: Option[Int] = None)(implicit timeOf: TimeExtractor[T]): StormSource[T] =
    SpoutSource(spout.map(t => (Timestamp(timeOf(t)), t)), defaultSourcePar.map(SourceParallelism(_)))

  implicit def spoutAsStormSource[T](spout: Spout[T])(implicit timeOf: TimeExtractor[T]): StormSource[T] =
    toStormSource(spout, None)(timeOf)

  def source[T](spout: Spout[T],
    defaultSourcePar: Option[Int] = None)(implicit timeOf: TimeExtractor[T]): Producer[Storm, T] =
    Producer.source[Storm, T](toStormSource(spout, defaultSourcePar))

  implicit def spoutAsSource[T](spout: Spout[T])(implicit timeOf: TimeExtractor[T]): Producer[Storm, T] =
    source(spout, None)(timeOf)
}

case class PlannedTopology(config: BacktypeStormConfig, topology: StormTopology)

abstract class Storm(options: Map[String, Options], transformConfig: SummingbirdConfig => SummingbirdConfig, passedRegistrars: List[IKryoRegistrar]) extends Platform[Storm] {
  @transient private val logger = LoggerFactory.getLogger(classOf[Storm])

  type Source[+T] = StormSource[T]
  type Store[-K, V] = MergeableStoreFactory[(K, BatchID), V]
  type Sink[-T] = StormSink[T]
  type Service[-K, +V] = OnlineServiceFactory[K, V]
  type Plan[T] = PlannedTopology

  private type Prod[T] = Producer[Storm, T]

  private[storm] def get[T <: AnyRef: Manifest](dag: Dag[Storm], node: StormNode): Option[(String, T)] = {
    val producer = node.members.last

    val namedNodes = dag.producerToPriorityNames(producer)
    (for {
      id <- namedNodes :+ "DEFAULT"
      stormOpts <- options.get(id)
      option <- stormOpts.get[T]
    } yield (id, option)).headOption
  }

  private[storm] def getOrElse[T <: AnyRef: Manifest](dag: Dag[Storm], node: StormNode, default: T): T = {
    get[T](dag, node) match {
      case None =>
        logger.debug("Node ({}): Using default setting {}", dag.getNodeName(node), default)
        default
      case Some((namedSource, option)) =>
        logger.info("Node {}: Using {} found via NamedProducer \"{}\"", Array[AnyRef](dag.getNodeName(node), option, namedSource))
        option
    }
  }

  /**
   * Set storm to tick our nodes every second to clean up finished futures
   */
  private def tickConfig = {
    val boltConfig = new BacktypeStormConfig
    boltConfig.put(BacktypeStormConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS, java.lang.Integer.valueOf(1))
    boltConfig
  }

  private def scheduleFlatMapper(jobID: JobId, stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val nodeName = stormDag.getNodeName(node)
    val usePreferLocalDependency = getOrElse(stormDag, node, DEFAULT_FM_PREFER_LOCAL_DEPENDENCY)
    logger.info("[{}] usePreferLocalDependency: {}", nodeName, usePreferLocalDependency.get)

    val bolt: BaseBolt[Any, Any] = FlatMapBoltProvider(this, jobID, stormDag, node).apply

    val parallelism = getOrElse(stormDag, node, DEFAULT_FM_PARALLELISM).parHint
    val declarer = topologyBuilder.setBolt(nodeName, bolt, parallelism).addConfigurations(tickConfig)

    val dependenciesNames = stormDag.dependenciesOf(node).collect { case x: StormNode => stormDag.getNodeName(x) }
    if (usePreferLocalDependency.get) {
      dependenciesNames.foreach { declarer.localOrShuffleGrouping(_) }
    } else {
      dependenciesNames.foreach { declarer.shuffleGrouping(_) }
    }
  }

  private def scheduleSpout[K](stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val (spout, parOpt) = node.members.collect { case Source(SpoutSource(s, parOpt)) => (s, parOpt) }.head
    val nodeName = stormDag.getNodeName(node)

    val tormentaSpout = node.members.reverse.foldLeft(spout.asInstanceOf[Spout[(Timestamp, Any)]]) { (spout, p) =>
      p match {
        case Source(_) => spout // The source is still in the members list so drop it
        case OptionMappedProducer(_, op) => spout.flatMap { case (time, t) => op.apply(t).map { x => (time, x) } }
        case NamedProducer(_, _) => spout
        case IdentityKeyedProducer(_) => spout
        case AlsoProducer(_, _) => spout
        case _ => sys.error("not possible, given the above call to span.\n" + p)
      }
    }

    val metrics = getOrElse(stormDag, node, DEFAULT_SPOUT_STORM_METRICS)

    val stormSpout = tormentaSpout.registerMetrics(metrics.toSpoutMetrics).getSpout
    val parallelism = getOrElse(stormDag, node, parOpt.getOrElse(DEFAULT_SOURCE_PARALLELISM)).parHint
    topologyBuilder.setSpout(nodeName, stormSpout, parallelism)
  }

  private def scheduleSummerBolt[K, V](jobID: JobId, stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val summer: Summer[Storm, K, V] = node.members.collect { case c: Summer[Storm, K, V] => c }.head
    implicit val semigroup = summer.semigroup
    implicit val batcher = summer.store.batcher
    val nodeName = stormDag.getNodeName(node)

    type ExecutorKeyType = (K, BatchID)
    type ExecutorValueType = (Timestamp, V)
    type ExecutorOutputType = (Timestamp, (K, (Option[V], V)))

    val supplier: MergeableStoreFactory[ExecutorKeyType, V] = summer.store match {
      case m: MergeableStoreFactory[ExecutorKeyType, V] => m
      case _ => sys.error("Should never be able to get here, looking for a MergeableStoreFactory from %s".format(summer.store))
    }

    val wrappedStore: MergeableStoreFactory[ExecutorKeyType, ExecutorValueType] =
      MergeableStoreFactoryAlgebra.wrapOnlineFactory(supplier)

    val anchorTuples = getOrElse(stormDag, node, AnchorTuples.default)
    val metrics = getOrElse(stormDag, node, DEFAULT_SUMMER_STORM_METRICS)
    val shouldEmit = stormDag.dependantsOf(node).size > 0

    val builder = BuildSummer(this, stormDag, node)

    val ackOnEntry = getOrElse(stormDag, node, DEFAULT_ACK_ON_ENTRY)
    logger.info("[{}] ackOnEntry : {}", nodeName, ackOnEntry.get)

    val maxEmitPerExecute = getOrElse(stormDag, node, DEFAULT_MAX_EMIT_PER_EXECUTE)
    logger.info("[{}] maxEmitPerExecute : {}", nodeName, maxEmitPerExecute.get)

    val maxExecutePerSec = getOrElse(stormDag, node, DEFAULT_MAX_EXECUTE_PER_SEC)
    logger.info("[{}] maxExecutePerSec : {}", nodeName, maxExecutePerSec.toString)

    val storeBaseFMOp = { op: (ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)) =>
      val ((k, batchID), (optiVWithTS, (ts, v))) = op
      val optiV = optiVWithTS.map(_._2)
      List((ts, (k, (optiV, v))))
    }

    val flatmapOp: FlatMapOperation[(ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)), ExecutorOutputType] =
      FlatMapOperation.apply(storeBaseFMOp)

    val sinkBolt = BaseBolt(
      jobID,
      metrics.metrics,
      anchorTuples,
      shouldEmit,
      new Fields(VALUE_FIELD),
      ackOnEntry,
      maxExecutePerSec,
      new executor.Summer(
        wrappedStore,
        flatmapOp,
        getOrElse(stormDag, node, DEFAULT_ONLINE_SUCCESS_HANDLER),
        getOrElse(stormDag, node, DEFAULT_ONLINE_EXCEPTION_HANDLER),
        builder,
        getOrElse(stormDag, node, DEFAULT_MAX_WAITING_FUTURES),
        getOrElse(stormDag, node, DEFAULT_MAX_FUTURE_WAIT_TIME),
        maxEmitPerExecute,
        getOrElse(stormDag, node, IncludeSuccessHandler.default),
        new KeyValueInjection[Int, CMap[ExecutorKeyType, ExecutorValueType]],
        new SingleItemInjection[ExecutorOutputType])
    )

    val parallelism = getOrElse(stormDag, node, DEFAULT_SUMMER_PARALLELISM).parHint
    val declarer =
      topologyBuilder.setBolt(
        nodeName,
        sinkBolt,
        parallelism
      ).addConfigurations(tickConfig)
    val dependenciesNames = stormDag.dependenciesOf(node).collect { case x: StormNode => stormDag.getNodeName(x) }
    dependenciesNames.foreach { parentName =>
      declarer.fieldsGrouping(parentName, new Fields(AGG_KEY))
    }

  }

  private def dumpOptions: String = {
    options.map {
      case (k, opts) =>
        "%s -> [%s]".format(k, opts.opts.values.mkString(", "))
    }.mkString("\n || ")
  }
  /**
   * The following operations are public.
   */

  /**
   * Base storm config instances used by the Storm platform.
   */

  def genConfig(dag: Dag[Storm]) = {
    val config = new BacktypeStormConfig
    config.setFallBackOnJavaSerialization(false)
    config.setKryoFactory(classOf[com.twitter.chill.storm.BlizzardKryoFactory])
    config.setMaxSpoutPending(1000)
    config.setNumAckers(12)
    config.setNumWorkers(12)

    val initialStormConfig = StormConfig(config)
    val stormConfig = SBChillRegistrar(initialStormConfig, passedRegistrars)
    logger.debug("Serialization config changes:")
    logger.debug("Removes: {}", stormConfig.removes)
    logger.debug("Updates: {}", stormConfig.updates)

    val inj = Injection.connect[String, Array[Byte], Base64String]
    logger.debug("Adding serialized copy of graphs")
    val withViz = stormConfig.put("summingbird.base64_graph.producer", inj.apply(VizGraph(dag.originalTail)).str)
      .put("summingbird.base64_graph.planned", inj.apply(VizGraph(dag)).str)

    val withOptions = withViz.put("summingbird.options", dumpOptions)
    val transformedConfig = transformConfig(withOptions)

    logger.debug("Config diff to be applied:")
    logger.debug("Removes: {}", transformedConfig.removes)
    logger.debug("Updates: {}", transformedConfig.updates)

    transformedConfig.removes.foreach(config.remove(_))
    transformedConfig.updates.foreach(kv => config.put(kv._1, kv._2))
    config
  }

  def withRegistrars(registrars: List[IKryoRegistrar]): Storm

  def withConfigUpdater(fn: SummingbirdConfig => SummingbirdConfig): Storm

  def plan[T](tail: TailProducer[Storm, T]): PlannedTopology = {
    /*
     * TODO: storm does not yet know about ValueFlatMapped, so remove it before
     * planning
     */
    val dagOptimizer = new DagOptimizer[Storm] {}
    val stormTail = dagOptimizer.optimize(tail, dagOptimizer.ValueFlatMapToFlatMap)
    val stormDag = OnlinePlan(stormTail.asInstanceOf[TailProducer[Storm, T]])
    implicit val topologyBuilder = new TopologyBuilder
    implicit val config = genConfig(stormDag)
    val jobID = JobId(config.get("storm.job.uniqueId").asInstanceOf[String])

    stormDag.nodes.foreach { node =>
      node match {
        case _: SummerNode[_] => scheduleSummerBolt(jobID, stormDag, node)
        case _: FlatMapNode[_] => scheduleFlatMapper(jobID, stormDag, node)
        case _: SourceNode[_] => scheduleSpout(stormDag, node)
      }
    }
    PlannedTopology(config, topologyBuilder.createTopology)
  }
  def run(tail: TailProducer[Storm, _], jobName: String): Unit = run(plan(tail), jobName)
  def run(plannedTopology: PlannedTopology, jobName: String): Unit
}

class RemoteStorm(options: Map[String, Options], transformConfig: SummingbirdConfig => SummingbirdConfig, passedRegistrars: List[IKryoRegistrar]) extends Storm(options, transformConfig, passedRegistrars) {

  override def withConfigUpdater(fn: SummingbirdConfig => SummingbirdConfig) =
    new RemoteStorm(options, transformConfig.andThen(fn), passedRegistrars)

  override def run(plannedTopology: PlannedTopology, jobName: String): Unit = {
    val topologyName = "summingbird_" + jobName
    StormSubmitter.submitTopology(topologyName, plannedTopology.config, plannedTopology.topology)
  }

  override def withRegistrars(registrars: List[IKryoRegistrar]) =
    new RemoteStorm(options, transformConfig, passedRegistrars ++ registrars)
}

class LocalStorm(options: Map[String, Options], transformConfig: SummingbirdConfig => SummingbirdConfig, passedRegistrars: List[IKryoRegistrar])
    extends Storm(options, transformConfig, passedRegistrars) {
  lazy val localCluster = new LocalCluster

  override def withConfigUpdater(fn: SummingbirdConfig => SummingbirdConfig) =
    new LocalStorm(options, transformConfig.andThen(fn), passedRegistrars)

  override def run(plannedTopology: PlannedTopology, jobName: String): Unit = {
    val topologyName = "summingbird_" + jobName
    localCluster.submitTopology(topologyName, plannedTopology.config, plannedTopology.topology)
  }

  override def withRegistrars(registrars: List[IKryoRegistrar]) =
    new LocalStorm(options, transformConfig, passedRegistrars ++ registrars)
}
