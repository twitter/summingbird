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

import backtype.storm.LocalCluster
import backtype.storm.Testing
import backtype.storm.testing.CompleteTopologyParam
import backtype.storm.testing.MockedSources
import backtype.storm.tuple.Fields
import backtype.storm.{Config, StormSubmitter}
import backtype.storm.generated.StormTopology
import backtype.storm.topology.{ BoltDeclarer, TopologyBuilder }
import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.storm.option.{ AnchorTuples, IncludeSuccessHandler }
import com.twitter.summingbird.util.CacheSize
import com.twitter.summingbird.kryo.KryoRegistrationHelper
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird._
import com.twitter.util.Future

import Constants._
import scala.annotation.tailrec

sealed trait StormNode {
  val members: Set[Producer[Storm, _]] = Set()

  private val dependantStateOpt = members.headOption.map(h => Dependants(h))

  def dependantsOf(p: Producer[Storm, _]): List[Producer[Storm, _]] = {
    dependantStateOpt match {
      case Some(dependantState) => dependantState.dependantsOf(p).getOrElse(List())
      case _ => List()
    }
  }

  def localDependantsOf(p: Producer[Storm, _]): List[Producer[Storm, _]] = dependantsOf(p).filter(members.contains(_))

  def toSpout: StormSpout = StormSpout(this.members)

  def toSummer: SummerStormBolt = SummerStormBolt(this.members)

  def contains(p: Producer[Storm, _]): Boolean = members.contains(p)

  def getName(): String = getClass.getName.replaceFirst("com.twitter.summingbird.storm.","")

  def add(node: Producer[Storm, _]): StormNode
}

case class IntermediateFlatMapStormBolt(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Intermediate Flatmap Bolt"
}

case class FinalFlatMapStormBolt(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Final Flatmap Bolt"
}

case class SummerStormBolt(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Summer Bolt"
}

case class StormSpout(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Spout"
}


case class StormDag(nodeLut: Map[Producer[Storm, _], StormNode], dag: Map[StormNode, Set[StormNode]] = Map[StormNode, Set[StormNode]](), allNodes: Set[StormNode] = Set[StormNode]()) {
  def connect(src: StormNode, dest: StormNode): StormDag = {
    if (src == dest) {
      this
    } else {
      assert(!dest.isInstanceOf[StormSpout])
      val currentTargets = dag.getOrElse(src, Set[StormNode]())
      StormDag(nodeLut, dag + (src -> (currentTargets + dest) ), allNodes)
    }
  }

  def nodes = allNodes

  def connect(src: Producer[Storm, _], dest: Producer[Storm, _]): StormDag = {
    val newDag = for {
      lNode <- nodeLut.get(src);
      rNode <- nodeLut.get(dest)
    } yield connect(lNode, rNode)
    newDag.getOrElse(this)
  }
  def dependantsOf(n: StormNode): Set[StormNode] = dag.get(n).getOrElse(Set())
}
object StormDag {
  def build(registry: StormRegistry) : StormDag = {
    val nodeLut = registry.buildLut
    registry.registry.foldLeft(StormDag(nodeLut, allNodes = registry.registry)){ (curDag, stormNode) =>
      stormNode.members.foldLeft(curDag) { (innerDag, outerProducer) =>
        outerProducer match {
          case Summer(producer, _, _) => innerDag.connect(producer, outerProducer)
          case IdentityKeyedProducer(producer) => innerDag.connect(producer, outerProducer)
          case NamedProducer(producer, newId) => innerDag.connect(producer, outerProducer)
          case OptionMappedProducer(producer, op, manifest) => innerDag.connect(producer, outerProducer)
          case FlatMappedProducer(producer, op) => innerDag.connect(producer, outerProducer)
          case WrittenProducer(producer, sinkSupplier) => innerDag.connect(producer, outerProducer)
          case LeftJoinedProducer(producer, StoreWrapper(newService)) => innerDag.connect(producer, outerProducer)
          case MergedProducer(l, r) => innerDag.connect(l, outerProducer).connect(r, outerProducer)
          case Source(_, _) => innerDag
        }
      }
    }
  }
}

case class StormRegistry(registry: Set[StormNode] = Set[StormNode]()) {
  
  def register(n: StormNode): StormRegistry =  {
    StormRegistry(registry + n )
  }

  def buildLut() : Map[Producer[Storm, _], StormNode] = {
    registry.foldLeft(Map[Producer[Storm, _], StormNode]()){ (curRegistry, stormNode) =>
      stormNode.members.foldLeft(curRegistry) { (innerRegistry, producer) =>
        (innerRegistry + (producer -> stormNode))
      }
    }
  }
}

object StormNode {

}
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

object Storm {
  def local(options: Map[String, Options] = Map.empty): LocalStorm =
    new LocalStorm(options, identity)

  def remote(options: Map[String, Options] = Map.empty): RemoteStorm =
    new RemoteStorm(options, identity)

  def timedSpout[T](spout: Spout[T])
    (implicit timeOf: TimeExtractor[T]): Spout[(Long, T)] =
    spout.map(t => (timeOf(t), t))

  def store[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreSupplier[K, V] =
    MergeableStoreSupplier.from(store)

  implicit def source[T: TimeExtractor: Manifest](spout: Spout[T]) =
    Producer.source[Storm, T](timedSpout(spout))
}

/**
  * Object containing helper functions to build up the list of storm
  * operations that can potentially be optimized.
  */
sealed trait FMItem
case class OptionMap[T, U](op: T => Option[U]) extends FMItem
case class FactoryCell(factory: StoreFactory[_, _]) extends FMItem
case class FlatMap(op: FlatMapOperation[_, _]) extends FMItem

object FMItem {
  def sink[T](sinkSupplier: () => (T => Future[Unit])): FMItem =
    FlatMap(FlatMapOperation.write(sinkSupplier))
}

abstract class Storm(options: Map[String, Options], updateConf: Config => Config) extends Platform[Storm] {
  type Source[+T] = Spout[(Long, T)]
  type Store[-K, V] = StormStore[K, V]
  type Sink[-T] = () => (T => Future[Unit])
  type Service[-K, +V] = StormService[K, V]
  type Plan[T] = StormTopology
  
  private type Prod[T] = Producer[Storm, T]
  private type VisitedStore = Set[Prod[_]]
  private type JamfMap = Map[Prod[_], List[String]]

  val END_SUFFIX = "end"
  val FM_CONSTANT = "flatMap-"

  /**
    * Returns true if this producer's suffix indicates that it's the
    * final flatMapper before the sumByKey call, false otherwise.
    */
  def isFinalFlatMap(suffix: String) = !suffix.contains(FM_CONSTANT)

  private def getOrElse[T: Manifest](idOpt: Option[String], default: T): T =
    (for {
      id <- idOpt
      stormOpts <- options.get(id)
      option <- stormOpts.get[T]
    } yield option).getOrElse(default)


  def collectPass[T](outerProducer: Prod[T], previousBolt: StormNode, stormRegistry: StormRegistry, forkedNodes: Set[Prod[_]], visited: VisitedStore): (StormRegistry, VisitedStore) = {
    
    val currentBolt = previousBolt.add(outerProducer)
    def recurse[U](
      producer: Prod[U],
      updatedBolt: StormNode = currentBolt,
      updatedDag: StormRegistry = stormRegistry,
      visited: VisitedStore = visited)
    : (StormRegistry, VisitedStore) = {
      collectPass(producer, updatedBolt, updatedDag, forkedNodes, visited = visited)
    }

    def maybeSplit[A](dependency: Prod[A], visited: VisitedStore): (StormRegistry, VisitedStore) = {
      if (forkedNodes.contains(dependency)) {
        recurse(dependency, updatedBolt = IntermediateFlatMapStormBolt(), updatedDag = stormRegistry.register(currentBolt), visited = visited)
      } else recurse(dependency, visited = visited)
    }
    
    if (visited.contains(outerProducer)) {
      (stormRegistry, visited)
    } else {
      val visitedWithN = visited + outerProducer
      outerProducer match {
        case Summer(producer, _, _) =>
          recurse(producer, updatedBolt = FinalFlatMapStormBolt(), updatedDag = stormRegistry.register(currentBolt.toSummer), visited = visitedWithN)

        case IdentityKeyedProducer(producer) => maybeSplit(producer, visited = visitedWithN)
        case NamedProducer(producer, newId) => maybeSplit(producer, visited = visitedWithN)
        case Source(spout, manifest) =>
          val spoutBolt = currentBolt.toSpout
          (stormRegistry.register(spoutBolt), visitedWithN)

        case OptionMappedProducer(producer, op, manifest) => maybeSplit(producer, visited = visitedWithN)

        case FlatMappedProducer(producer, op)  => maybeSplit(producer, visited = visitedWithN)

        case WrittenProducer(producer, sinkSupplier)  => maybeSplit(producer, visited = visitedWithN)

        case LeftJoinedProducer(producer, StoreWrapper(newService)) => maybeSplit(producer, visited = visitedWithN)

        case MergedProducer(l, r) =>
          val (lDag, newVisisted) = recurse(l, updatedBolt = IntermediateFlatMapStormBolt(), updatedDag = stormRegistry.register(currentBolt), visited = visitedWithN)
          recurse(r, updatedBolt = IntermediateFlatMapStormBolt(), updatedDag = lDag, visited = newVisisted)
      }

    }
  }

  def buildTopology[T](
    topoBuilder: TopologyBuilder,
    outerProducer: Prod[T],
    forkedNodes: Set[Prod[_]],
    jamfs: JamfMap,
    toSchedule: List[FMItem],
    path: List[Prod[_]],
    suffix: String,
    id: Option[String])
    (implicit config: Config): (List[String], JamfMap) = {

    /**
      * Helper method for recursively calling this same function with
      * all of its arguments set, by default, to the current call's
      * supplied parameters. (Note that this recursion will loop
      * infinitely if called directly with no changed parameters.)
      */
    def recurse[U](
      producer: Prod[U],
      topoBuilder: TopologyBuilder = topoBuilder,
      outerProducer: Prod[T] = outerProducer,
      jamfs: JamfMap = jamfs,
      toSchedule: List[FMItem] = toSchedule,
      path: List[Prod[_]] = path,
      suffix: String = suffix,
      id: Option[String] = id) =
      buildTopology(
        topoBuilder, producer, forkedNodes, jamfs,
        toSchedule, outerProducer :: path, suffix, id
      )

    /**
      * TODO: This internal check is duplicating the internal isEmpty
      * check in scheduleFlatMapper. The idea is that we should only
      * tag a flatMap- prefix on if there are operations to
      * schedule. The path is calculated in a separate spot from the
      * actual scheduling.
      *
      * Remove the duplication and do everything in one spot by
      * changing the return type of scheduleFlatMapper.
      */
    def suffixOf(xs: List[_], suffix: String): String =
      if (xs.isEmpty) suffix else FM_CONSTANT + suffix

    def flatMap(parents: List[String], ops: List[FMItem]) =
      scheduleFlatMapper(topoBuilder, parents, path, suffix, id, ops)

    /**
      * This method is called by any nodes that contain Storm
      * FlatMapOperations. If the calling node is a "forked node" (ie,
      * has multiple consumers), we can't do any optimization across
      * the boundary, so we schedule a flatMap operation and push the
      * contained "op" down into the recursion. If it's not a forked
      * node, we can continue to optimize the graph by pushing the
      * current operation onto the toSchedule stack.
      */
    def perhapsSchedule[A](parent: Prod[A], op: FMItem) = {
      val newOps = op :: toSchedule
      if (forkedNodes.contains(parent)) {
        val (s, m) = recurse(
          parent,
          toSchedule = List.empty,
          suffix = "fork-" + suffixOf(newOps, suffix)
        )
        (flatMap(s, newOps), m)
      } else recurse(parent, toSchedule = newOps)
    }

    jamfs.get(outerProducer) match {
      case Some(s) => (s, jamfs)
      case None =>
        val (strings, m): (List[String], JamfMap) = outerProducer match {
          case Summer(producer, _, _) =>
            assert(path.isEmpty, "Only a single Summer is supported at this time.")
            recurse(producer)

          case IdentityKeyedProducer(producer) => recurse(producer)

          case NamedProducer(producer, newId) => recurse(producer, id = Some(newId))

          case Source(spout, manifest) =>
            // The current planner requires a layer of flatMapBolts, even
            // if calling sumByKey directly on a source.
            val (optionMaps, remaining) = toSchedule.span {
              case OptionMap(_) => true
              case _ => false
            }

            val operations =
              if (remaining.isEmpty)
                List(FlatMap(FlatMapOperation.identity))
              else remaining

            val spoutName = "spout-" + suffixOf(operations, suffix)

            val stormSpout = optionMaps.foldLeft(spout.asInstanceOf[Spout[(Long, Any)]]) {
              case (spout, OptionMap(op)) =>
                spout.flatMap { case (time, t) =>
                  op.asInstanceOf[Any => Option[_]].apply(t)
                    .map { x => (time, x) } }
              case _ => sys.error("not possible, given the above call to span.")
            }.getSpout
            val parallelism = getOrElse(id, DEFAULT_SPOUT_PARALLELISM).parHint
            topoBuilder.setSpout(spoutName, stormSpout, parallelism)
            val parents = List(spoutName)

            // Attach a FlatMapBolt after this source.
            (flatMap(parents, operations), jamfs)

          case OptionMappedProducer(producer, op, manifest) =>
            perhapsSchedule(producer, OptionMap(op))

          case FlatMappedProducer(producer, op) =>
            perhapsSchedule(producer, FlatMap(FlatMapOperation(op)))

          case WrittenProducer(producer, sinkSupplier) =>
            perhapsSchedule(producer, FMItem.sink(sinkSupplier))

          case LeftJoinedProducer(producer, StoreWrapper(newService)) =>
            perhapsSchedule(producer, FactoryCell(newService))

          case MergedProducer(l, r) =>
            val leftSuffix = "L-" + suffixOf(toSchedule, suffix)
            val rightSuffix = "R-" + suffixOf(toSchedule, suffix)
            val (leftNodes, leftM) =
              recurse(l, toSchedule = List.empty, suffix = leftSuffix)
            val (rightNodes, rightM) =
              recurse(r, toSchedule = List.empty, suffix = rightSuffix, jamfs = leftM)
            val parents = leftNodes ++ rightNodes
            (flatMap(parents, toSchedule), rightM)
        }
        (strings, m + (outerProducer -> strings))
    }
  }

  /**
    * Only exists because of the crazy casts we needed.
    */
  private def serviceOperation[K, V, W](store: StoreFactory[_, _]) =
    FlatMapOperation.combine(
      FlatMapOperation.identity[(K, V)],
      store.asInstanceOf[StoreFactory[K, W]]
    )

  private def foldOperations(head: FMItem, tail: List[FMItem]) = {
    val operation = head match {

      case OptionMap(op) => FlatMapOperation(op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]])
      case FactoryCell(store) => serviceOperation(store)
      case FlatMap(op) => op
    }
    tail.foldLeft(operation.asInstanceOf[FlatMapOperation[Any, Any]]) {
      case (acc, FactoryCell(store)) => FlatMapOperation.combine(
        acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
        store.asInstanceOf[StoreFactory[Any, Any]]
      ).asInstanceOf[FlatMapOperation[Any, Any]]
      case (acc, OptionMap(op)) => acc.andThen(FlatMapOperation[Any, Any](op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]]))
      case (acc, FlatMap(op)) => acc.andThen(op.asInstanceOf[FlatMapOperation[Any, Any]])
    }
  }

  // TODO https://github.com/twitter/summingbird/issues/84: This
  // function is returning the Node ID; replace string programming
  // with a world where the "id" is actually the path to that node
  // from the root.
  private def scheduleFlatMapper(
    topoBuilder: TopologyBuilder,
    parents: List[String],
    path: List[Prod[_]],
    suffix: String,
    id: Option[String],
    toSchedule: List[FMItem])
      : List[String] = {
    toSchedule match {
      case Nil => parents
      case head :: tail =>
        val operation = foldOperations(head, tail)
        val metrics = getOrElse(id, DEFAULT_FM_STORM_METRICS)
        val anchorTuples = getOrElse(id, AnchorTuples.default)

        val bolt = if (isFinalFlatMap(suffix)) {
          val summer = Producer.retrieveSummer(path)
            .getOrElse(sys.error("A Summer is required."))
          new FinalFlatMapBolt(
            operation.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
            getOrElse(id, DEFAULT_FM_CACHE),
            getOrElse(id, DEFAULT_FM_STORM_METRICS),
            anchorTuples
          )(summer.monoid.asInstanceOf[Monoid[Any]], summer.store.batcher)
        }
        else
          new IntermediateFlatMapBolt(operation, metrics, anchorTuples)

        val parallelism = getOrElse(id, DEFAULT_FM_PARALLELISM)
        val boltName = FM_CONSTANT + suffix
        val declarer = topoBuilder.setBolt(boltName, bolt, parallelism.parHint)

        parents.foreach { declarer.shuffleGrouping(_) }
        List(boltName)
    }
  }

  private def populate[K, V](
    topologyBuilder: TopologyBuilder,
    summer: Summer[Storm, K, V],
    name: Option[String])(implicit config: Config) = {
    implicit val monoid = summer.monoid

    val dep = Dependants(summer)
    val fanOutSet =
      Producer.transitiveDependenciesOf(summer)
        .filter(dep.fanOut(_).exists(_ > 1)).toSet

    val (parents, _) = buildTopology(
      topologyBuilder, summer, fanOutSet, Map.empty,
      List.empty, List.empty,
      END_SUFFIX, name)
    val supplier = summer.store match {
      case MergeableStoreSupplier(contained, _) => contained
    }

    val sinkBolt = new SinkBolt[K, V](
      supplier,
      getOrElse(name, DEFAULT_ONLINE_SUCCESS_HANDLER),
      getOrElse(name, DEFAULT_ONLINE_EXCEPTION_HANDLER),
      getOrElse(name, DEFAULT_SINK_CACHE),
      getOrElse(name, DEFAULT_SINK_STORM_METRICS),
      getOrElse(name, DEFAULT_MAX_WAITING_FUTURES),
      getOrElse(name, IncludeSuccessHandler.default)
    )

    val declarer =
      topologyBuilder.setBolt(
        GROUP_BY_SUM,
        sinkBolt,
        getOrElse(name, DEFAULT_SINK_PARALLELISM).parHint)

    parents.foreach { parentName =>
      declarer.fieldsGrouping(parentName, new Fields(AGG_KEY))
    }
    List(GROUP_BY_SUM)
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
    config.setKryoFactory(classOf[SummingbirdKryoFactory])
    config.setMaxSpoutPending(1000)
    config.setNumAckers(12)
    config.setNumWorkers(12)
    transformConfig(config)
  }

  def transformConfig(base: Config): Config = updateConf(base)
  def withConfigUpdater(fn: Config => Config): Storm

  def plan[T](node: Producer[Storm, T]): StormTopology = {
    val topologyBuilder = new TopologyBuilder
    implicit val config = baseConfig

    /**
      * This crippled version of the StormPlatform only supports a
      * Summer or any number of NamedProducers stacked onto the end of
      * the DAG.
      */
    @tailrec def retrieve(p: Producer[Storm, _], id: Option[String] = None): (Summer[Storm, Any, Any], Option[String]) =
      p match {
        case s: Summer[Storm, Any, Any] => (s, id)
        case NamedProducer(inner, name) => retrieve(inner, Some(name))
        case _ => sys.error("A Summer is required.")
      }
    val (summer, name) = retrieve(node)

    // TODO (https://github.com/twitter/summingbird/issues/86):
    // support topologies that don't end with a sum
    populate(topologyBuilder, summer, name)
    topologyBuilder.createTopology
  }
  def run(summer: Producer[Storm, _], jobName: String): Unit = run(plan(summer), jobName)
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
