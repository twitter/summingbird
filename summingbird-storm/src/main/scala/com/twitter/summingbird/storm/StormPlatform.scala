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

import backtype.storm.tuple.Fields
import backtype.storm.{Config, StormSubmitter}
import backtype.storm.generated.StormTopology
import backtype.storm.topology.{ BoltDeclarer, TopologyBuilder }
import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.Constants._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.builder.{FlatMapOption, FlatMapParallelism, IncludeSuccessHandler, SinkOption, SinkParallelism}
import com.twitter.summingbird.util.{ CacheSize, KryoRegistrationHelper }
import com.twitter.tormenta.spout.ScalaSpout
import com.twitter.summingbird._

case class StormSerialization[T](injectionPair: InjectionPair[T]) extends Serialization[Storm, T]

sealed trait StormStore[K, V] extends Store[Storm, K, V]
case class MergeableStoreSupplier[K, V](store: () => MergeableStore[(K, BatchID), V]) extends StormStore[K, V]

sealed trait StormService[K, V] extends Service[Storm, K, V]
case class StoreWrapper[K, V](store: () => ReadableStore[K, V]) extends StormService[K, V]

/**
  * intra-graph options.
  */
class StormOptions(opts: Map[Class[_], Any] = Map.empty) {
  def set(opt: SinkOption) = new StormOptions(opts + (opt.getClass -> opt))
  def set(opt: FlatMapOption) = new StormOptions(opts + (opt.getClass -> opt))
  def set(opt: CacheSize) = new StormOptions(opts + (opt.getClass -> opt))

  def get[T: Manifest]: Option[T] =
    opts.get(manifest[T].erasure).asInstanceOf[Option[T]]

  def getOrElse[T: Manifest](default: T): T =
    opts.getOrElse(manifest[T].erasure, default).asInstanceOf[T]
}

object Storm {
  val SINK_ID = "sinkId"

  def retrieveSummer[K, V](paths: List[Producer[Storm, _]]): Option[Summer[Storm, K, V]] =
    paths match {
      case Nil => None
      case (node: Summer[Storm, K, V]) :: _ => Some(node)
      case _ :: tail => retrieveSummer(tail)
    }

  def source[T](spout: ScalaSpout[T])
    (implicit inj: Injection[T, Array[Byte]], manifest: Manifest[T], timeOf: TimeExtractor[T]) =
    Producer.source[Storm, T, ScalaSpout[T]](spout)

  // TODO: Add an unapply that pulls the spout out of the source,
  // casting appropriately.
  implicit def ser[T](implicit inj: Injection[T, Array[Byte]], mf: Manifest[T])
      : Serialization[Storm, T] = {
    StormSerialization(InjectionPair(mf.erasure.asInstanceOf[Class[T]], inj))
  }

}

class Storm(jobName: String, options: Map[String, StormOptions]) extends Platform[Storm] {
  import Storm.SINK_ID

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
    toSchedule: List[Either[() => ReadableStore[_, _], FlatMapOperation[_, _]]],
    path: List[Producer[Storm, _]],
    suffix: String,
    id: Option[String])
    (implicit config: Config): List[String] = {

    def recurse[T, U](
      producer: Producer[Storm, U],
      topoBuilder: TopologyBuilder = topoBuilder,
      outerProducer: Producer[Storm, T] = outerProducer,
      toSchedule: List[Either[() => ReadableStore[_, _], FlatMapOperation[_, _]]] = toSchedule,
      path: List[Producer[Storm, _]] = path,
      suffix: String = suffix,
      id: Option[String] = id) =
      buildTopology(topoBuilder, producer, toSchedule, outerProducer :: path, suffix, id)

    def suffixOf[T](xs: List[T], suffix: String): String =
      if (xs.isEmpty) suffix else FM_CONSTANT + suffix

    outerProducer match {
      case Summer(producer, _, _, _, _, _, _) => {
        assert(path.isEmpty, "Only a single Summer is supported at this time.")
        recurse(producer)
      }
      case IdentityKeyedProducer(producer) => recurse(producer)
      case NamedProducer(producer, newId)  => recurse(producer, id = Some(newId))
      case Source(source, ser, timeOf) => {
        // Register this source's serialization in the config.
        KryoRegistrationHelper.registerInjections(
          config,
          Some(ser.asInstanceOf[StormSerialization[T]].injectionPair)
        )
        val spoutName = "spout-" + suffixOf(toSchedule, suffix)
        val spout = source.asInstanceOf[ScalaSpout[T]]
        val stormSpout = spout.getSpout { scheme =>
          scheme.map { t =>
            val batcher =
              Storm.retrieveSummer(path).map { s: Summer[Storm, _, _] => s.batcher }
                .getOrElse(sys.error("No summer found!"))

            val batch = batcher.batchOf(new java.util.Date(timeOf(t)))
            (batch.id, t)
          }
        }
        topoBuilder.setSpout(spoutName, stormSpout, spout.parallelism)
        val parents = List(spoutName)

        // The current planner requires a layer of flatMapBolts, even
        // if calling sumByKey directly on a source.
        val operations = if (toSchedule.isEmpty) List(Right(ident)) else toSchedule

        // Attach a FlatMapBolt after this source.
        scheduleFlatMapper(topoBuilder, parents, path, suffix, id, operations)
      }

      case FlatMappedProducer(producer, op) => {
        val newOp = FlatMapOperation(op)
        recurse(producer, toSchedule = Right(newOp) :: toSchedule)
      }

      case LeftJoinedProducer(producer, svc) => {
        val newService =
          svc.asInstanceOf[StormService[_, _]] match {
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

  def ident[T] = FlatMapOperation { t: T => Some(t) }

  def serviceOperation[K, V, W](store: () => ReadableStore[_, _]) =
    FlatMapOperation.combine(ident[(K, V)], store.asInstanceOf[() => ReadableStore[K, W]])

  def combine[T, K, V, W](op: FlatMapOperation[T, (K, V)], store: () => ReadableStore[_, _]): FlatMapOperation[_, _] =
    FlatMapOperation.combine(
      op,
      store.asInstanceOf[() => ReadableStore[K, W]]
    )

  def foldOperations(
    head: Either[() => ReadableStore[_, _], FlatMapOperation[_, _]],
    tail: List[Either[() => ReadableStore[_, _], FlatMapOperation[_, _]]]) = {
    val operation = head match {
      case Left(store) => serviceOperation(store)
      case Right(op) => op
    }

    tail.foldLeft(operation.asInstanceOf[FlatMapOperation[Any, Any]]) {
      case (acc, Left(store)) => combine(acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]], store).asInstanceOf[FlatMapOperation[Any, Any]]
      case (acc, Right(op)) => acc.andThen(op.asInstanceOf[FlatMapOperation[Any, Any]])
    }
  }

  // TODO: This function is returning the Node ID; replace string
  // programming with a world where the "id" is actually the path to
  // that node from the root.
  def scheduleFlatMapper(
    topoBuilder: TopologyBuilder,
    parents: List[String],
    path: List[Producer[Storm, _]],
    suffix: String,
    id: Option[String],
    toSchedule: List[Either[() => ReadableStore[_, _], FlatMapOperation[_, _]]])
      : List[String] = {
    toSchedule match {
      case Nil => parents
      case head :: tail => {
        val summer = Storm.retrieveSummer(path).getOrElse(sys.error("A Summer is required."))
        val boltName = FM_CONSTANT + suffix
        val operation = foldOperations(head, tail)
        val metrics = getOrElse(id, DEFAULT_FM_STORM_METRICS)
        val bolt = if (isFinalFlatMap(suffix))
          new FinalFlatMapBolt(
            operation.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
            getOrElse(id, DEFAULT_FM_CACHE),
            getOrElse(id, DEFAULT_FM_STORM_METRICS)
          )(summer.monoid.asInstanceOf[Monoid[Any]], summer.batcher)
        else
          new FMBolt(operation, metrics)

        val parallelism = getOrElse(id, DEFAULT_FM_PARALLELISM)
        val declarer = topoBuilder.setBolt(boltName, bolt, parallelism.parHint)

        parents.foreach { declarer.shuffleGrouping(_) }
        List(boltName)
      }
    }
  }

  def baseConfig = {
    val config = new Config
    config.setFallBackOnJavaSerialization(false)
    config.setKryoFactory(classOf[SummingbirdKryoFactory])
    config.setMaxSpoutPending(1000)
    config.setNumAckers(12)
    config.setNumWorkers(12)
    config
  }

  /**
    * TODO: Completed is really still a producer. We can submit
    * topologies at the completed nodes, but otherwise they can
    * continue to flatMap, etc.
    */
  def populate[K, V](
    topologyBuilder: TopologyBuilder,
    summer: Summer[Storm, K, V])(implicit config: Config) = {
    implicit val batcher = summer.batcher
    implicit val monoid  = summer.monoid

    // Register the K and V serializations in the config.
    KryoRegistrationHelper.registerInjectionDefaults(
      config,
      List(
        summer.kSer.asInstanceOf[StormSerialization[K]].injectionPair,
        summer.vSer.asInstanceOf[StormSerialization[V]].injectionPair
      ))

    val parents = buildTopology(topologyBuilder, summer, List.empty, List.empty, END_SUFFIX, None)
    // TODO: Add wrapping case classes for memstore, etc, as in MemP.
    val supplier = summer.store.asInstanceOf[StormStore[K, V]] match {
      case MergeableStoreSupplier(contained) => contained
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

  def run[K, V](summer: Summer[Storm, K, V]): Unit = {
    val topologyBuilder = new TopologyBuilder
    implicit val config = baseConfig
    populate(topologyBuilder, summer)
    StormSubmitter.submitTopology(
      "summingbird_" + jobName,
      new Config,
      topologyBuilder.createTopology
    )
  }
}
