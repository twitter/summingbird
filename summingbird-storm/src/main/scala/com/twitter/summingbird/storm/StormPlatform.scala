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

import Constants._
import org.apache.storm.{ LocalCluster, StormSubmitter, Config => BacktypeStormConfig }
import com.twitter.bijection.{ Base64String, Injection }
import com.twitter.chill.IKryoRegistrar
import com.twitter.storehaus.algebra.{ Mergeable, MergeableStore }
import com.twitter.storehaus.{ ReadableStore, WritableStore }
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.chill.SBChillRegistrar
import com.twitter.summingbird.online._
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner._
import com.twitter.summingbird.storm.option.AnchorTuples
import com.twitter.summingbird.storm.planner.StormNode
import com.twitter.summingbird.viz.VizGraph
import com.twitter.tormenta.spout.Spout
import com.twitter.util.Future
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.slf4j.LoggerFactory
import scala.collection.{ Map => CMap }
import scala.reflect.ClassTag

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

  /**
   * Returns a store that is also a service, i.e. is a ReadableStore[K, V] and a Mergeable[(K, BatchID), V]
   * The values used for the service are from the online store only.
   * Uses ClientStore internally to create ReadableStore[K, V]
   */
  def storeServiceOnlineOnly[K, V](store: => MergeableStore[(K, BatchID), V], batchesToKeep: Int)(implicit batcher: Batcher): CombinedServiceStoreFactory[K, V] =
    CombinedServiceStoreFactory(store, batchesToKeep)(batcher)

  /**
   * Returns a store that is also a service, i.e. is a ReadableStore[K, V] and a Mergeable[(K, BatchID), V]
   * The values used for the service are from the online *and* offline stores.
   * Uses ClientStore internally to combine the offline and online stores to create ReadableStore[K, V]
   */
  def storeService[K, V](offlineStore: => ReadableStore[K, (BatchID, V)], onlineStore: => MergeableStore[(K, BatchID), V], batchesToKeep: Int)(implicit batcher: Batcher): CombinedServiceStoreFactory[K, V] =
    CombinedServiceStoreFactory(offlineStore, onlineStore, batchesToKeep)(batcher)

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

  private[storm] def get[T <: AnyRef: ClassTag](dag: Dag[Storm], node: StormNode): Option[(String, T)] = {
    val producer = node.members.last
    Options.getFirst[T](options, dag.producerToPriorityNames(producer))
  }

  private[storm] def getOrElse[T <: AnyRef: ClassTag](dag: Dag[Storm], node: StormNode, default: T): T =
    get[T](dag, node) match {
      case None =>
        logger.debug(s"Node (${dag.getNodeName(node)}): Using default setting $default")
        default
      case Some((namedSource, option)) =>
        logger.info(s"Node ${dag.getNodeName(node)}: Using $option found via NamedProducer ${'"'}$namedSource${'"'}")
        option
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
    logger.info(s"[$nodeName] usePreferLocalDependency: ${usePreferLocalDependency.get}")

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

  private def scheduleSpout(jobID: JobId, stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val nodeName = stormDag.getNodeName(node)
    val (sourceParalleism, stormSpout) = SpoutProvider(this, stormDag, node, jobID).apply
    topologyBuilder.setSpout(nodeName, stormSpout, sourceParalleism)
  }

  private def scheduleSummerBolt[K, V](jobID: JobId, stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val summer: Summer[Storm, K, V] = node.members.collect { case c: Summer[Storm, K, V] => c }.head
    implicit val semigroup = summer.semigroup
    implicit val batcher = summer.store.mergeableBatcher
    val nodeName = stormDag.getNodeName(node)

    type ExecutorKeyType = (K, BatchID)
    type ExecutorValueType = (Timestamp, V)
    type ExecutorOutputType = (Timestamp, (K, (Option[V], V)))

    val anchorTuples = getOrElse(stormDag, node, AnchorTuples.default)
    val metrics = getOrElse(stormDag, node, DEFAULT_SUMMER_STORM_METRICS)
    val shouldEmit = stormDag.dependantsOf(node).size > 0

    val builder = BuildSummer(this, stormDag, node, jobID)

    val ackOnEntry = getOrElse(stormDag, node, DEFAULT_ACK_ON_ENTRY)
    logger.info(s"[$nodeName] ackOnEntry : ${ackOnEntry.get}")

    val maxEmitPerExecute = getOrElse(stormDag, node, DEFAULT_MAX_EMIT_PER_EXECUTE)
    logger.info(s"[$nodeName] maxEmitPerExecute : ${maxEmitPerExecute.get}")

    val maxExecutePerSec = getOrElse(stormDag, node, DEFAULT_MAX_EXECUTE_PER_SEC)
    logger.info(s"[$nodeName] maxExecutePerSec : $maxExecutePerSec")

    val storeBaseFMOp = { op: (ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)) =>
      val ((k, batchID), (optiVWithTS, (ts, v))) = op
      val optiV = optiVWithTS.map(_._2)
      List((ts, (k, (optiV, v))))
    }

    val flatmapOp: FlatMapOperation[(ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)), ExecutorOutputType] =
      FlatMapOperation.apply(storeBaseFMOp)

    val supplier: MergeableStoreFactory[ExecutorKeyType, V] = summer.store

    val sinkBolt = BaseBolt(
      jobID,
      metrics.metrics,
      anchorTuples,
      shouldEmit,
      new Fields(VALUE_FIELD),
      ackOnEntry,
      maxExecutePerSec,
      new KeyValueInjection[Int, CMap[ExecutorKeyType, ExecutorValueType]],
      new SingleItemInjection[ExecutorOutputType],
      new executor.Summer(
        () => new WrappedTSInMergeable(supplier.mergeableStore(semigroup)),
        flatmapOp,
        getOrElse(stormDag, node, DEFAULT_ONLINE_SUCCESS_HANDLER),
        getOrElse(stormDag, node, DEFAULT_ONLINE_EXCEPTION_HANDLER),
        builder,
        getOrElse(stormDag, node, DEFAULT_MAX_WAITING_FUTURES),
        getOrElse(stormDag, node, DEFAULT_MAX_FUTURE_WAIT_TIME),
        maxEmitPerExecute,
        getOrElse(stormDag, node, IncludeSuccessHandler.default))
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
    val stormDag = OnlinePlan(stormTail.asInstanceOf[TailProducer[Storm, T]], options)
    implicit val topologyBuilder = new TopologyBuilder
    implicit val config = genConfig(stormDag)
    val jobID = {
      val stormJobId = config.get("storm.job.uniqueId").asInstanceOf[String]
      if (stormJobId == null) {
        JobId(java.util.UUID.randomUUID().toString + "-AutoGenerated-ForInbuiltStats-UserStatsWillFail.")
      } else {
        JobId(stormJobId)
      }
    }

    stormDag.nodes.foreach { node =>
      node match {
        case _: SummerNode[_] => scheduleSummerBolt(jobID, stormDag, node)
        case _: FlatMapNode[_] => scheduleFlatMapper(jobID, stormDag, node)
        case _: SourceNode[_] => scheduleSpout(jobID, stormDag, node)
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
