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
import com.twitter.summingbird.Constants._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.storm.option.IncludeSuccessHandler
import com.twitter.summingbird.util.CacheSize
import com.twitter.summingbird.kryo.KryoRegistrationHelper
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird._
import com.twitter.util.Future

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
  val SINK_ID = "sinkId"

  def local(name: String, options: Map[String, Options] = Map.empty): LocalStorm =
    new LocalStorm(name, options, identity)

  def remote(name: String, options: Map[String, Options] = Map.empty): RemoteStorm =
    new RemoteStorm(name, options, identity)

  def timedSpout[T](spout: Spout[T])
    (implicit timeOf: TimeExtractor[T]): Spout[(Long, T)] =
    spout.map(t => (timeOf(t), t))

  implicit def source[T: TimeExtractor: Manifest](spout: Spout[T]) =
    Producer.source[Storm, T](timedSpout(spout))
}

abstract class Storm(options: Map[String, Options], updateConf: Config => Config) extends Platform[Storm] {
  import Storm.SINK_ID

  type Source[+T] = Spout[(Long, T)]
  type Store[-K, V] = StormStore[K, V]
  type Sink[-T] = () => (T => Future[Unit])
  type Service[-K, +V] = StormService[K, V]
  type Plan[T] = StormTopology

  private type Prod[T] = Producer[Storm, T]
  private type JamfMap = Map[Prod[_], List[String]]
  private type FMItem = Either[StoreFactory[_, _], FlatMapOperation[_, _]]
  private type FMList = List[FMItem]

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

  def buildTopology[T](
    topoBuilder: TopologyBuilder,
    outerProducer: Prod[T],
    forkedNodes: Set[Prod[_]],
    jamfs: JamfMap,
    toSchedule: FMList,
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
      toSchedule: FMList = toSchedule,
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

    def flatMap[T, U](parents: List[String], ops: FMList) =
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
    def perhapsSchedule[A, B](parent: Prod[A], op: FMItem) =
      if (forkedNodes.contains(outerProducer)) {
        val (s, m) = recurse(parent, toSchedule = List(op))
        (flatMap(s, toSchedule), m)
      } else
        recurse(parent, toSchedule = op :: toSchedule)

    jamfs.get(outerProducer) match {
      case Some(s) => (s, jamfs)
      case None =>
        val (strings, m): (List[String], JamfMap) = outerProducer match {
          case Summer(producer, _, _) =>
            assert(path.isEmpty, "Only a single Summer is supported at this time.")
            recurse(producer)

          case IdentityKeyedProducer(producer) => recurse(producer)

          case NamedProducer(producer, newId) => recurse(producer, id = Some(newId))

          case Source(mappedSpout, manifest) =>
            val spoutName = "spout-" + suffixOf(toSchedule, suffix)
            val stormSpout = mappedSpout.getSpout
            val parallelism = getOrElse(id, DEFAULT_SPOUT_PARALLELISM).parHint
            topoBuilder.setSpout(spoutName, stormSpout, parallelism)
            val parents = List(spoutName)

            // The current planner requires a layer of flatMapBolts, even
            // if calling sumByKey directly on a source.
            val operations =
              if (toSchedule.isEmpty)
                List(Right(FlatMapOperation.identity))
              else toSchedule

            // Attach a FlatMapBolt after this source.
            (flatMap(parents, operations), jamfs)

          case OptionMappedProducer(producer, op, manifest) =>
            perhapsSchedule(producer, Right(FlatMapOperation(op.andThen(_.iterator))))

          case FlatMappedProducer(producer, op) =>
            perhapsSchedule(producer, Right(FlatMapOperation(op)))

          case WrittenProducer(producer, sinkSupplier) =>
            perhapsSchedule(producer, Right(FlatMapOperation.write(sinkSupplier)))

          case LeftJoinedProducer(producer, svc) =>
            val newService = svc match {
              case StoreWrapper(storeSupplier) => storeSupplier
            }
            perhapsSchedule(producer, Left(newService))

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

  private def foldOperations(
    head: Either[StoreFactory[_, _], FlatMapOperation[_, _]],
    tail: List[Either[StoreFactory[_, _], FlatMapOperation[_, _]]]) = {
    val operation = head match {
      case Left(store) => serviceOperation(store)
      case Right(op) => op
    }
    tail.foldLeft(operation.asInstanceOf[FlatMapOperation[Any, Any]]) {
      case (acc, Left(store)) => FlatMapOperation.combine(
        acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
        store.asInstanceOf[StoreFactory[Any, Any]]
      ).asInstanceOf[FlatMapOperation[Any, Any]]
      case (acc, Right(op)) => acc.andThen(op.asInstanceOf[FlatMapOperation[Any, Any]])
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
    toSchedule: List[Either[StoreFactory[_, _], FlatMapOperation[_, _]]])
      : List[String] = {
    toSchedule match {
      case Nil => parents
      case head :: tail =>
        val summer = Producer.retrieveSummer(path)
          .getOrElse(sys.error("A Summer is required."))
        val boltName = FM_CONSTANT + suffix
        val operation = foldOperations(head, tail)
        val metrics = getOrElse(id, DEFAULT_FM_STORM_METRICS)
        val bolt = if (isFinalFlatMap(suffix))
          new FinalFlatMapBolt(
            operation.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
            getOrElse(id, DEFAULT_FM_CACHE),
            getOrElse(id, DEFAULT_FM_STORM_METRICS)
          )(summer.monoid.asInstanceOf[Monoid[Any]], summer.store.batcher)
        else
          new IntermediateFlatMapBolt(operation, metrics)

        val parallelism = getOrElse(id, DEFAULT_FM_PARALLELISM)
        val declarer = topoBuilder.setBolt(boltName, bolt, parallelism.parHint)

        parents.foreach { declarer.shuffleGrouping(_) }
        List(boltName)
    }
  }

  private def populate[K, V](
    topologyBuilder: TopologyBuilder,
    summer: Summer[Storm, K, V])(implicit config: Config) = {
    implicit val monoid = summer.monoid

    val dep = Dependants(summer)
    val fanOutSet =
      Producer.transitiveDependenciesOf(summer)
        .filter(dep.fanOut(_).exists(_ > 1))

    val (parents, _) = buildTopology(
      topologyBuilder, summer, fanOutSet, Map.empty,
      List.empty, List.empty,
      END_SUFFIX, None)
    val supplier = summer.store match {
      case MergeableStoreSupplier(contained, _) => contained
    }

    val idOpt = Some(SINK_ID)
    val sinkBolt = new SinkBolt[K, V](
      supplier,
      getOrElse(idOpt, DEFAULT_ONLINE_SUCCESS_HANDLER),
      getOrElse(idOpt, DEFAULT_ONLINE_EXCEPTION_HANDLER),
      getOrElse(idOpt, DEFAULT_SINK_CACHE),
      getOrElse(idOpt, DEFAULT_SINK_STORM_METRICS),
      getOrElse(idOpt, DEFAULT_MAX_WAITING_FUTURES),
      getOrElse(idOpt, IncludeSuccessHandler(true))
    )

    val declarer =
      topologyBuilder.setBolt(
        GROUP_BY_SUM,
        sinkBolt,
        getOrElse(idOpt, DEFAULT_SINK_PARALLELISM).parHint)

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

  def plan[T](summer: Producer[Storm, T]): StormTopology = {
    val topologyBuilder = new TopologyBuilder
    implicit val config = baseConfig

    // TODO (https://github.com/twitter/summingbird/issues/86):
    // support topologies that don't end with a sum
    populate(topologyBuilder, summer.asInstanceOf[Summer[Storm,Any,Any]])
    topologyBuilder.createTopology
  }
  def run(summer: Producer[Storm, _]): Unit = run(plan(summer))
  def run(topology: StormTopology): Unit
}

class RemoteStorm(jobName: String, options: Map[String, Options], updateConf: Config => Config)
    extends Storm(options, updateConf) {

  override def withConfigUpdater(fn: Config => Config) =
    new RemoteStorm(jobName, options, updateConf.andThen(fn))

  def run(topology: StormTopology): Unit = {
    val topologyName = "summingbird_" + jobName
    StormSubmitter.submitTopology(topologyName, baseConfig, topology)
  }
}

class LocalStorm(jobName: String, options: Map[String, Options], updateConf: Config => Config)
    extends Storm(options, updateConf) {
  lazy val localCluster = new LocalCluster

  override def withConfigUpdater(fn: Config => Config) =
    new LocalStorm(jobName, options, updateConf.andThen(fn))

  def run(topology: StormTopology): Unit = {
    val topologyName = "summingbird_" + jobName
    localCluster.submitTopology(topologyName, baseConfig, topology)
  }
}
