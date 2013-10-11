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
import backtype.storm.{Config, LocalCluster, StormSubmitter}
import backtype.storm.generated.StormTopology
import backtype.storm.topology.{BoltDeclarer, TopologyBuilder}
import backtype.storm.tuple.Fields
import com.twitter.algebird.Monoid
import com.twitter.chill.ScalaKryoInstantiator
import com.twitter.chill.config.{ ConfiguredInstantiator, JavaMapConfig }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.storm.option.{AnchorTuples, IncludeSuccessHandler}
import com.twitter.summingbird.util.CacheSize
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird.planner._
import com.twitter.summingbird.storm.planner._
import com.twitter.util.Future
import scala.annotation.tailrec

sealed trait StormStore[-K, V] {
  def batcher: Batcher
}

object MergeableStoreSupplier {
  def from[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreSupplier[K, V] =
    MergeableStoreSupplier(() => store, batcher)
}

case class MergeableStoreSupplier[K, V](store: () => MergeableStore[(K, BatchID), V], batcher: Batcher) extends StormStore[K, V]

sealed trait StormService[-K, +V]
case class StoreWrapper[K, V](store: StoreFactory[K, V]) extends StormService[K, V]

sealed trait StormSource[+T]
case class SpoutSource[+T](spout: Spout[(Long, T)]) extends StormSource[T]

object Storm {
  def local(options: Map[String, Options] = Map.empty): LocalStorm =
    new LocalStorm(options, identity)

  def remote(options: Map[String, Options] = Map.empty): RemoteStorm =
    new RemoteStorm(options, identity)

  def store[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreSupplier[K, V] =
    MergeableStoreSupplier.from(store)

  implicit def toStormSource[T](spout: Spout[T])(implicit timeOf: TimeExtractor[T]) = 
    SpoutSource(spout.map(t => (timeOf(t), t)))

  implicit def source[T](spout: Spout[T])(implicit timeOf: TimeExtractor[T]) =
    Producer.source[Storm, T](toStormSource(spout))
}

abstract class Storm(options: Map[String, Options], updateConf: Config => Config) extends Platform[Storm] {
  type Source[+T] = StormSource[T]
  type Store[-K, V] = StormStore[K, V]
  type Sink[-T] = () => (T => Future[Unit])
  type Service[-K, +V] = StormService[K, V]
  type Plan[T] = StormTopology

  private type Prod[T] = Producer[Storm, T]

  private def getOrElse[T: Manifest](dag: Dag[Storm], node: StormNode, default: T): T = {
    val producer = node.members.last
    val namedNodes = dag.transitiveDependantsOf(producer).collect{case NamedProducer(_, n) => n}
    (for {
      id <- namedNodes
      stormOpts <- options.get(id)
      option <- stormOpts.get[T]
    } yield option).headOption.getOrElse(default)
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
              val newService = wrapper.asInstanceOf[StoreWrapper[Any, Any]].store
              FlatMapOperation.combine(
                acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
                newService.asInstanceOf[StoreFactory[Any, Any]]).asInstanceOf[FlatMapOperation[Any, Any]]
            case OptionMappedProducer(_, op) => acc.andThen(FlatMapOperation[Any, Any](op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]]))
            case FlatMappedProducer(_, op) => acc.andThen(FlatMapOperation(op).asInstanceOf[FlatMapOperation[Any, Any]])
            case WrittenProducer(_, sinkSupplier) => acc.andThen(FlatMapOperation.write(sinkSupplier.asInstanceOf[() => (Any => Future[Unit])]))
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

    val summerOpt:Option[SummerNode[Storm]] = stormDag.dependantsOf(node).collect{case s: SummerNode[Storm] => s}.headOption
    
    val bolt = summerOpt match {
      case Some(s) =>
        val summerProducer = s.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Storm, _, _]]
        new FinalFlatMapBolt(
          operation.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
          getOrElse(stormDag, node, DEFAULT_FM_CACHE),
          getOrElse(stormDag, node, DEFAULT_FM_STORM_METRICS),
          anchorTuples)(summerProducer.monoid.asInstanceOf[Monoid[Any]], summerProducer.store.batcher)
      case None =>
        new IntermediateFlatMapBolt(operation, metrics, anchorTuples, stormDag.dependenciesOf(node).size > 0)
    }

    val parallelism = getOrElse(stormDag, node, DEFAULT_FM_PARALLELISM)
    val declarer = topologyBuilder.setBolt(nodeName, bolt, parallelism.parHint)


    val dependenciesNames = stormDag.dependenciesOf(node).collect { case x: StormNode => stormDag.getNodeName(x) }
    dependenciesNames.foreach { declarer.shuffleGrouping(_) }
  }

  private def scheduleSpout[K](stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val spout = node.members.collect { case Source(SpoutSource(s)) => s }.head
    val nodeName = stormDag.getNodeName(node)

    val stormSpout = node.members.reverse.foldLeft(spout.asInstanceOf[Spout[(Long, Any)]]) { (spout, p) =>
      p match {
        case Source(_) => spout // The source is still in the members list so drop it
        case OptionMappedProducer(_, op) => spout.flatMap {case (time, t) => op.apply(t).map { x => (time, x) }}
        case NamedProducer(_, _) => spout
        case IdentityKeyedProducer(_) => spout
        case AlsoProducer(_, _) => spout
        case _ => sys.error("not possible, given the above call to span.\n" + p)
      }
    }.getSpout

    val parallelism = getOrElse(stormDag, node, DEFAULT_SPOUT_PARALLELISM).parHint
    topologyBuilder.setSpout(nodeName, stormSpout, parallelism)
  }

  private def scheduleSinkBolt[K, V](stormDag: Dag[Storm], node: StormNode)(implicit topologyBuilder: TopologyBuilder) = {
    val summer: Summer[Storm, K, V] = node.members.collect { case c: Summer[Storm, K, V] => c }.head
    implicit val monoid = summer.monoid
    val nodeName = stormDag.getNodeName(node)

    val supplier = summer.store match {
      case MergeableStoreSupplier(contained, _) => contained
    }

    val sinkBolt = new SinkBolt[K, V](
      supplier,
      getOrElse(stormDag, node, DEFAULT_ONLINE_SUCCESS_HANDLER),
      getOrElse(stormDag, node, DEFAULT_ONLINE_EXCEPTION_HANDLER),
      getOrElse(stormDag, node, DEFAULT_SINK_CACHE),
      getOrElse(stormDag, node, DEFAULT_SINK_STORM_METRICS),
      getOrElse(stormDag, node, DEFAULT_MAX_WAITING_FUTURES),
      getOrElse(stormDag, node, IncludeSuccessHandler.default))

    val declarer =
      topologyBuilder.setBolt(
        nodeName,
        sinkBolt,
        getOrElse(stormDag, node, DEFAULT_SINK_PARALLELISM).parHint)
    val dependenciesNames = stormDag.dependenciesOf(node).collect { case x: StormNode => stormDag.getNodeName(x) }
    dependenciesNames.foreach { parentName =>
      declarer.fieldsGrouping(parentName, new Fields(AGG_KEY))
    }

  }

  /**
   * The following operations are public.
   */

  /**
   * Base storm config instances used by the Storm platform.
   */
  def baseConfig = {
    val config = new Config
    config.setFallBackOnJavaSerialization(false)
    config.setKryoFactory(classOf[com.twitter.chill.storm.BlizzardKryoFactory])
    config.setMaxSpoutPending(1000)
    config.setNumAckers(12)
    config.setNumWorkers(12)
    val kryoConfig = new JavaMapConfig(config)
    ConfiguredInstantiator.setSerialized(
      kryoConfig,
      classOf[ScalaKryoInstantiator],
      new ScalaKryoInstantiator()
    )
    transformConfig(config)
  }

  def transformConfig(base: Config): Config = updateConf(base)
  def withConfigUpdater(fn: Config => Config): Storm

  def plan[T](tail: TailProducer[Storm, T]): StormTopology = {
    implicit val topologyBuilder = new TopologyBuilder
    implicit val config = baseConfig

    val stormDag = OnlinePlan(tail)

    stormDag.nodes.foreach { node =>
      node match {
        case _: SummerNode[_] => scheduleSinkBolt(stormDag, node)
        case _: FlatMapNode[_] => scheduleFlatMapper(stormDag, node)
        case _: SourceNode[_] => scheduleSpout(stormDag, node)
      }
    }
    topologyBuilder.createTopology
  }
  def run(summer: TailProducer[Storm, _], jobName: String): Unit = run(plan(summer), jobName)
  def run(topology: StormTopology, jobName: String): Unit
}

class RemoteStorm(options: Map[String, Options], updateConf: Config => Config) extends Storm(options, updateConf) {

  override def withConfigUpdater(fn: Config => Config) =
    new RemoteStorm(options, updateConf.andThen(fn))

  override def run(topology: StormTopology, jobName: String): Unit = {
    val topologyName = "summingbird_" + jobName
    StormSubmitter.submitTopology(topologyName, baseConfig, topology)
  }
}

class LocalStorm(options: Map[String, Options], updateConf: Config => Config)
  extends Storm(options, updateConf) {
  lazy val localCluster = new LocalCluster

  override def withConfigUpdater(fn: Config => Config) =
    new LocalStorm(options, updateConf.andThen(fn))

  override def run(topology: StormTopology, jobName: String): Unit = {
    val topologyName = "summingbird_" + jobName
    localCluster.submitTopology(topologyName, baseConfig, topology)
  }
}
