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
import com.twitter.summingbird.viz.VizGraph
import com.twitter.tormenta.spout.Spout
import com.twitter.util.Future
import org.apache.storm.generated.StormTopology
import org.slf4j.LoggerFactory

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
    val stormDag = OnlinePlan(tail, options)
    val config = genConfig(stormDag)
    val jobId = {
      val stormJobId = config.get("storm.job.uniqueId").asInstanceOf[String]
      if (stormJobId == null) {
        JobId(java.util.UUID.randomUUID().toString + "-AutoGenerated-ForInbuiltStats-UserStatsWillFail.")
      } else {
        JobId(stormJobId)
      }
    }

    val topology = StormTopologyBuilder(options, jobId, stormDag).build
    PlannedTopology(config, topology.build(jobId))
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
