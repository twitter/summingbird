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
import com.twitter.summingbird.builder.{FlatMapOption, FlatMapParallelism, IncludeSuccessHandler, SinkOption, SinkParallelism}
import com.twitter.summingbird.util.CacheSize
import com.twitter.summingbird.kryo.KryoRegistrationHelper
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird._

sealed trait StormStore[-K, V] {
  def batcher: Batcher
}

object MergeableStoreSupplier {
  def apply[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreSupplier[K, V] =
    MergeableStoreSupplier(() => store, batcher)
}

case class MergeableStoreSupplier[K, V](store: () => MergeableStore[(K, BatchID), V], batcher: Batcher) extends StormStore[K, V]

sealed trait StormService[-K, +V]
case class StoreWrapper[K, V](store: StoreFactory[K, V]) extends StormService[K, V]

object Storm {
  val SINK_ID = "sinkId"

  def local(name: String, options: Map[String, StormOptions] = Map.empty) =
    new LocalStorm(name, options)

  def apply(options: Map[String, StormOptions] = Map.empty): Storm =
    new Storm(options)

  implicit def source[T](spout: Spout[T])
    (implicit inj: Injection[T, Array[Byte]], manifest: Manifest[T], timeOf: TimeExtractor[T]) = {
    val mappedSpout = spout.map { t => (timeOf(t), t) }
    Producer.source[Storm, T](mappedSpout)
  }
}

abstract class Storm(options: Map[String, StormOptions]) extends Platform[Storm] {
  import Storm.SINK_ID

  type Source[+T] = Spout[(Long, T)]
  type Store[-K, V] = StormStore[K, V]
  type Service[-K, +V] = StormService[K, V]
  type Plan[T] = StormTopology

  val END_SUFFIX = "end"
  val FM_CONSTANT = "flatMap-"

  /**
    * Returns true if this producer's suffix indicates that it's the
    * final flatMapper before the sumByKey call, false otherwise.
    */
  def isFinalFlatMap(suffix: String) = !suffix.contains(FM_CONSTANT)

  def getOrElse[T: Manifest](idOpt: Option[String], default: T): T =
    (for {
      id <- idOpt
      stormOpts <- options.get(id)
      option <- stormOpts.get[T]
    } yield option).getOrElse(default)

  def buildTopology[T](
    topoBuilder: TopologyBuilder,
    outerProducer: Producer[Storm, T],
    toSchedule: List[Either[StoreFactory[_, _], FlatMapOperation[_, _]]],
    path: List[Producer[Storm, _]],
    suffix: String,
    id: Option[String])
    (implicit config: Config): List[String] = {

    /**
      * Helper method for recursively calling this same function with
      * all of its arguments set, by default, to the current call's
      * supplied parameters. (Note that this recursion will loop
      * infinitely if called directly with no changed parameters.)
      */
    def recurse[U](
      producer: Producer[Storm, U],
      topoBuilder: TopologyBuilder = topoBuilder,
      outerProducer: Producer[Storm, T] = outerProducer,
      toSchedule: List[Either[StoreFactory[_, _], FlatMapOperation[_, _]]] = toSchedule,
      path: List[Producer[Storm, _]] = path,
      suffix: String = suffix,
      id: Option[String] = id) =
      buildTopology(topoBuilder, producer, toSchedule, outerProducer :: path, suffix, id)

    def suffixOf[T](xs: List[T], suffix: String): String =
      if (xs.isEmpty) suffix else FM_CONSTANT + suffix

    outerProducer match {
      case Summer(producer, _, _) => {
        assert(path.isEmpty, "Only a single Summer is supported at this time.")
        recurse(producer)
      }
      case IdentityKeyedProducer(producer) => recurse(producer)
      case NamedProducer(producer, newId)  => recurse(producer, id = Some(newId))
      case Source(mappedSpout, manifest) => {
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
        scheduleFlatMapper(topoBuilder, parents, path, suffix, id, operations)
      }

      case OptionMappedProducer(producer, op, manifest) => {
        // TODO: we should always push this to the spout
        val newOp = FlatMapOperation(op andThen { _.iterator })
        recurse(producer, toSchedule = Right(newOp) :: toSchedule)
      }

      case FlatMappedProducer(producer, op) => {
        val newOp = FlatMapOperation(op)
        recurse(producer, toSchedule = Right(newOp) :: toSchedule)
      }

      case LeftJoinedProducer(producer, svc) => {
        val newService = svc match {
          case StoreWrapper(storeSupplier) => storeSupplier
        }
        recurse(producer, toSchedule = Left(newService) :: toSchedule)
      }

      case MergedProducer(l, r) => {
        val leftSuffix = "L-" + suffixOf(toSchedule, suffix)
        val rightSuffix = "R-" + suffixOf(toSchedule, suffix)
        val leftNodes  = recurse(l, toSchedule = List.empty, suffix = leftSuffix)
        val rightNodes = recurse(r, toSchedule = List.empty, suffix = rightSuffix)
        val parents = leftNodes ++ rightNodes
        scheduleFlatMapper(topoBuilder, parents, path, suffix, id, toSchedule)
      }
    }
  }

  /**
    * Only exists because of the crazy casts we needed.
    */
  private def serviceOperation[K, V, W](store: StoreFactory[_, _]) =
    FlatMapOperation.combine(FlatMapOperation.identity[(K, V)], store.asInstanceOf[StoreFactory[K, W]])

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

  // TODO: This function is returning the Node ID; replace string
  // programming with a world where the "id" is actually the path to
  // that node from the root.
  private def scheduleFlatMapper(
    topoBuilder: TopologyBuilder,
    parents: List[String],
    path: List[Producer[Storm, _]],
    suffix: String,
    id: Option[String],
    toSchedule: List[Either[StoreFactory[_, _], FlatMapOperation[_, _]]])
      : List[String] = {
    toSchedule match {
      case Nil => parents
      case head :: tail => {
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
  }

  /**
    * TODO: Completed is really still a producer. We can submit
    * topologies at the completed nodes, but otherwise they can
    * continue to flatMap, etc.
    */
  private def populate[K, V](
    topologyBuilder: TopologyBuilder,
    summer: Summer[Storm, K, V])(implicit config: Config) = {
    implicit val monoid  = summer.monoid

    val parents = buildTopology(topologyBuilder, summer, List.empty, List.empty, END_SUFFIX, None)
    // TODO: Add wrapping case classes for memstore, etc, as in MemP.
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
      getOrElse(idOpt, IncludeSuccessHandler(false))
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
    config
  }

  def plan[T](summer: Producer[Storm, T]): StormTopology = {
    val topologyBuilder = new TopologyBuilder
    implicit val config = baseConfig

    // TODO support topologies that don't end with a sum
    populate(topologyBuilder, summer.asInstanceOf[Summer[Storm,Any,Any]])
    topologyBuilder.createTopology
  }
}

class RemoteStorm(jobName: String, options: Map[String, StormOptions]) extends Storm(options) {
  def run(topology: StormTopology): Unit = {
    val topologyName = "summingbird_" + jobName
    StormSubmitter.submitTopology(topologyName, baseConfig, topology)
  }
}

class LocalStorm(jobName: String, options: Map[String, StormOptions]) extends Storm(options) {
  lazy val localCluster = new LocalCluster

  def run(topology: StormTopology): Unit = {
    val topologyName = "summingbird_" + jobName
    localCluster.submitTopology(topologyName, baseConfig, topology)
  }
}
