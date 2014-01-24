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
import backtype.storm.{Config => BacktypeStormConfig, LocalCluster, StormSubmitter}
import backtype.storm.generated.StormTopology
import backtype.storm.topology.{BoltDeclarer, TopologyBuilder}
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple

import com.twitter.bijection.{Base64String, Injection}
import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.chill.IKryoRegistrar
import com.twitter.storehaus.{ReadableStore, WritableStore}
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird._
import com.twitter.summingbird.viz.VizGraph
import com.twitter.summingbird.chill._
import com.twitter.summingbird.batch.{BatchID, Batcher, Timestamp}
import com.twitter.summingbird.storm.option.{AckOnEntry, AnchorTuples}
import com.twitter.summingbird.online.{MultiTriggerCache, SummingQueueCache}
import com.twitter.summingbird.online.executor.InputState
import com.twitter.summingbird.online.option.{IncludeSuccessHandler, MaxWaitingFutures, MaxFutureWaitTime}
import com.twitter.summingbird.util.CacheSize
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird.planner._
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.storm.planner._
import com.twitter.util.Future
import scala.annotation.tailrec
import backtype.storm.tuple.Values
import org.slf4j.LoggerFactory

/*
 * Batchers are used for partial aggregation. We never aggregate past two items which are not in the same batch.
 * This is needed/used everywhere we partially aggregate, summer's into stores, map side partial aggregation before summers, etc..
 */

sealed trait StormStore[-K, V] {
  def batcher: Batcher
}

object MergeableStoreSupplier {
  def from[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreSupplier[K, V] =
    MergeableStoreSupplier(() => store, batcher)

   def fromOnlineOnly[K, V](store: => MergeableStore[K, V]): MergeableStoreSupplier[K, V] = {
    implicit val batcher = Batcher.unit
    from(store.convert{k: (K, BatchID) => k._1})
  }
}


case class MergeableStoreSupplier[K, V](store: () => MergeableStore[(K, BatchID), V], batcher: Batcher) extends StormStore[K, V]

trait StormService[-K, +V] {
  def store: StoreFactory[K, V]
}

case class StoreWrapper[K, V](store: StoreFactory[K, V]) extends StormService[K, V]

sealed trait StormSource[+T]
case class SpoutSource[+T](spout: Spout[(Timestamp, T)], parallelism: Option[option.SpoutParallelism]) extends StormSource[T]

object Storm {
  def local(options: Map[String, Options] = Map.empty): LocalStorm =
    new LocalStorm(options, identity, List())

  def remote(options: Map[String, Options] = Map.empty): RemoteStorm =
    new RemoteStorm(options, identity, List())

  /**
   * Below are factory methods for the input output types:
   */

  def sink[T](fn: => T => Future[Unit]): Storm#Sink[T] = new SinkFn(fn)

  def sinkIntoWritable[K,V](store: => WritableStore[K, V]): Storm#Sink[(K,V)] =
    new WritableStoreSink[K, V](store)

  // This can be used in jobs that do not have a batch component
  def onlineOnlyStore[K, V](store: => MergeableStore[K, V]): StormStore[K, V] =
    MergeableStoreSupplier.fromOnlineOnly(store)

  def store[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): StormStore[K, V] =
    MergeableStoreSupplier.from(store)

  def service[K, V](serv: => ReadableStore[K, V]): StormService[K, V] = StoreWrapper(() => serv)

  def toStormSource[T](spout: Spout[T],
                       defaultSourcePar: Option[Int] = None)(implicit timeOf: TimeExtractor[T]): StormSource[T] =
    SpoutSource(spout.map(t => (Timestamp(timeOf(t)), t)), defaultSourcePar.map(option.SpoutParallelism(_)))

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
  type Store[-K, V] = StormStore[K, V]
  type Sink[-T] = StormSink[T]
  type Service[-K, +V] = StormService[K, V]
  type Plan[T] = PlannedTopology

  private type Prod[T] = Producer[Storm, T]

  private def getOrElse[T <: AnyRef : Manifest](dag: Dag[Storm], node: StormNode, default: T): T = {
    val producer = node.members.last

    val namedNodes = dag.producerToPriorityNames(producer)
    val maybePair = (for {
      id <- namedNodes :+ "DEFAULT"
      stormOpts <- options.get(id)
      option <- stormOpts.get[T]
    } yield (id, option)).headOption

    maybePair match {
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

  private def scheduleFlatMapper(stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    /**
     * Only exists because of the crazy casts we needed.
     */
    def foldOperations(producers: List[Producer[Storm, _]]): FlatMapOperation[Any, Any] = {
      producers.foldLeft(FlatMapOperation.identity[Any]) {
        case (acc, p) =>
          p match {
            case LeftJoinedProducer(_, wrapper) =>
              val newService = wrapper.store
              FlatMapOperation.combine(
                acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
                newService.asInstanceOf[StoreFactory[Any, Any]]).asInstanceOf[FlatMapOperation[Any, Any]]
            case OptionMappedProducer(_, op) => acc.andThen(FlatMapOperation[Any, Any](op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]]))
            case FlatMappedProducer(_, op) => acc.andThen(FlatMapOperation(op).asInstanceOf[FlatMapOperation[Any, Any]])
            case WrittenProducer(_, sinkSupplier) =>
              acc.andThen(FlatMapOperation.write(() => sinkSupplier.toFn))
            case IdentityKeyedProducer(_) => acc
            case MergedProducer(_, _) => acc
            case NamedProducer(_, _) => acc
            case AlsoProducer(_, _) => acc
            case Source(_) => sys.error("Should not schedule a source inside a flat mapper")
            case Summer(_, _, _) => sys.error("Should not schedule a Summer inside a flat mapper")
            case KeyFlatMappedProducer(_, op) => acc.andThen(FlatMapOperation.keyFlatMap[Any, Any, Any](op).asInstanceOf[FlatMapOperation[Any, Any]])
          }
      }
    }
    val nodeName = stormDag.getNodeName(node)
    val operation = foldOperations(node.members.reverse)
    val metrics = getOrElse(stormDag, node, DEFAULT_FM_STORM_METRICS)
    val anchorTuples = getOrElse(stormDag, node, AnchorTuples.default)
    logger.info("[{}] Anchoring: {}", nodeName, anchorTuples.anchor)

    val maxWaiting = getOrElse(stormDag, node, DEFAULT_MAX_WAITING_FUTURES)
    val maxWaitTime = getOrElse(stormDag, node, DEFAULT_MAX_FUTURE_WAIT_TIME)
    logger.info("[{}] maxWaiting: {}", nodeName, maxWaiting.get)

    val usePreferLocalDependency = getOrElse(stormDag, node, DEFAULT_FM_PREFER_LOCAL_DEPENDENCY)
    logger.info("[{}] usePreferLocalDependency: {}", nodeName, usePreferLocalDependency.get)

    val flushFrequency = getOrElse(stormDag, node, DEFAULT_FLUSH_FREQUENCY)
    logger.info("[{}] maxWaiting: {}", nodeName, flushFrequency.get)

    val cacheSize = getOrElse(stormDag, node, DEFAULT_FM_CACHE)
    logger.info("[{}] cacheSize lowerbound: {}", nodeName, cacheSize.lowerBound)

    val summerOpt:Option[SummerNode[Storm]] = stormDag.dependantsOf(node).collect{case s: SummerNode[Storm] => s}.headOption

    val useAsyncCache = getOrElse(stormDag, node, DEFAULT_USE_ASYNC_CACHE)
    logger.info("[{}] useAsyncCache : {}", nodeName, useAsyncCache.get)

    val ackOnEntry = getOrElse(stormDag, node, DEFAULT_ACK_ON_ENTRY)
    logger.info("[{}] ackOnEntry : {}", nodeName, ackOnEntry.get)

    val bolt = summerOpt match {
      case Some(s) =>
        val summerProducer = s.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Storm, _, _]]
        val cacheBuilder = if(useAsyncCache.get) {
          val softMemoryFlush = getOrElse(stormDag, node, DEFAULT_SOFT_MEMORY_FLUSH_PERCENT)
          logger.info("[{}] softMemoryFlush : {}", nodeName, softMemoryFlush.get)

          val asyncPoolSize = getOrElse(stormDag, node, DEFAULT_ASYNC_POOL_SIZE)
          logger.info("[{}] asyncPoolSize : {}", nodeName, asyncPoolSize.get)

          val valueCombinerCrushSize = getOrElse(stormDag, node, DEFAULT_VALUE_COMBINER_CACHE_SIZE)
          logger.info("[{}] valueCombinerCrushSize : {}", nodeName, valueCombinerCrushSize.get)

          MultiTriggerCache.builder[(Any, BatchID), (List[InputState[Tuple]], Timestamp, Any)](cacheSize, valueCombinerCrushSize, flushFrequency, softMemoryFlush, asyncPoolSize)
        } else {
          SummingQueueCache.builder[(Any, BatchID), (List[InputState[Tuple]], Timestamp, Any)](cacheSize, flushFrequency)
        }

        BaseBolt(
          metrics.metrics,
          anchorTuples,
          true,
          new Fields(AGG_KEY, AGG_VALUE),
          ackOnEntry,
          new executor.FinalFlatMap(
            operation.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
            cacheBuilder,
            maxWaiting,
            maxWaitTime,
            new SingleItemInjection[Any],
            new KeyValueInjection[(Any, BatchID), Any]
            )(summerProducer.monoid.asInstanceOf[Monoid[Any]], summerProducer.store.batcher)
            )
      case None =>
      BaseBolt(
          metrics.metrics,
          anchorTuples,
          stormDag.dependantsOf(node).size > 0,
          new Fields(VALUE_FIELD),
          ackOnEntry,
          new executor.IntermediateFlatMap(
            operation,
            maxWaiting,
            maxWaitTime,
            new SingleItemInjection[Any],
            new SingleItemInjection[Any]
            )
          )
    }

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
        case OptionMappedProducer(_, op) => spout.flatMap {case (time, t) => op.apply(t).map { x => (time, x) }}
        case NamedProducer(_, _) => spout
        case IdentityKeyedProducer(_) => spout
        case AlsoProducer(_, _) => spout
        case _ => sys.error("not possible, given the above call to span.\n" + p)
      }
    }

    val metrics = getOrElse(stormDag, node, DEFAULT_SPOUT_STORM_METRICS)
    tormentaSpout.registerMetrics(metrics.toSpoutMetrics)

    val stormSpout = tormentaSpout.getSpout
    val parallelism = getOrElse(stormDag, node, parOpt.getOrElse(DEFAULT_SPOUT_PARALLELISM)).parHint
    topologyBuilder.setSpout(nodeName, stormSpout, parallelism)
  }

  private def scheduleSummerBolt[K, V](stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val summer: Summer[Storm, K, V] = node.members.collect { case c: Summer[Storm, K, V] => c }.head
    implicit val monoid = summer.monoid
    val nodeName = stormDag.getNodeName(node)

    val supplier = summer.store match {
      case MergeableStoreSupplier(contained, _) => contained
    }
    val anchorTuples = getOrElse(stormDag, node, AnchorTuples.default)
    val metrics = getOrElse(stormDag, node, DEFAULT_SUMMER_STORM_METRICS)
    val shouldEmit = stormDag.dependantsOf(node).size > 0

    val flushFrequency = getOrElse(stormDag, node, DEFAULT_FLUSH_FREQUENCY)
    logger.info("[{}] maxWaiting: {}", nodeName, flushFrequency.get)

    val cacheSize = getOrElse(stormDag, node, DEFAULT_FM_CACHE)
    logger.info("[{}] cacheSize lowerbound: {}", nodeName, cacheSize.lowerBound)

    val ackOnEntry = getOrElse(stormDag, node, DEFAULT_ACK_ON_ENTRY)
    logger.info("[{}] ackOnEntry : {}", nodeName, ackOnEntry.get)

    val useAsyncCache = getOrElse(stormDag, node, DEFAULT_USE_ASYNC_CACHE)
    logger.info("[{}] useAsyncCache : {}", nodeName, useAsyncCache.get)

    val cacheBuilder = if(useAsyncCache.get) {
          val softMemoryFlush = getOrElse(stormDag, node, DEFAULT_SOFT_MEMORY_FLUSH_PERCENT)
          logger.info("[{}] softMemoryFlush : {}", nodeName, softMemoryFlush.get)

          val asyncPoolSize = getOrElse(stormDag, node, DEFAULT_ASYNC_POOL_SIZE)
          logger.info("[{}] asyncPoolSize : {}", nodeName, asyncPoolSize.get)

          val valueCombinerCrushSize = getOrElse(stormDag, node, DEFAULT_VALUE_COMBINER_CACHE_SIZE)
          logger.info("[{}] valueCombinerCrushSize : {}", nodeName, valueCombinerCrushSize.get)

          MultiTriggerCache.builder[(K, BatchID), (List[InputState[Tuple]], Timestamp, V)](cacheSize, valueCombinerCrushSize, flushFrequency, softMemoryFlush, asyncPoolSize)
        } else {
          SummingQueueCache.builder[(K, BatchID), (List[InputState[Tuple]], Timestamp, V)](cacheSize, flushFrequency)
        }

    val sinkBolt = BaseBolt(
          metrics.metrics,
          anchorTuples,
          shouldEmit,
          new Fields(VALUE_FIELD),
          ackOnEntry,
          new executor.Summer(
              supplier,
              getOrElse(stormDag, node, DEFAULT_ONLINE_SUCCESS_HANDLER),
              getOrElse(stormDag, node, DEFAULT_ONLINE_EXCEPTION_HANDLER),
              cacheBuilder,
              getOrElse(stormDag, node, DEFAULT_MAX_WAITING_FUTURES),
              getOrElse(stormDag, node, DEFAULT_MAX_FUTURE_WAIT_TIME),
              getOrElse(stormDag, node, IncludeSuccessHandler.default),
              new KeyValueInjection[(K,BatchID), V],
              new SingleItemInjection[(K, (Option[V], V))])
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
    options.map{case (k, opts) =>
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
    val stormDag = OnlinePlan(tail)
    implicit val topologyBuilder = new TopologyBuilder
    implicit val config = genConfig(stormDag)


    stormDag.nodes.foreach { node =>
      node match {
        case _: SummerNode[_] => scheduleSummerBolt(stormDag, node)
        case _: FlatMapNode[_] => scheduleFlatMapper(stormDag, node)
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
