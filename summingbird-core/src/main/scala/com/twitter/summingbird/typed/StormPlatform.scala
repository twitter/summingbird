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

package com.twitter.summingbird.typed

import backtype.storm.{Config, StormSubmitter}
import backtype.storm.generated.StormTopology
import backtype.storm.topology.{BoltDeclarer, IRichBolt, TopologyBuilder}
import com.twitter.bijection.Injection
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.Constants
import com.twitter.summingbird.Constants._
import com.twitter.summingbird.FunctionFlatMapper
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.builder.{FlatMapOption, FlatMapParallelism, IncludeSuccessHandler, SinkOption, SinkParallelism}
import com.twitter.summingbird.scalding.BatchAggregatorJob
import com.twitter.summingbird.scalding.store.IntermediateStore
import com.twitter.summingbird.storm.{DecoderBolt, FMBolt, FlatMapBolt, PairedBolt, SinkBolt}
import com.twitter.summingbird.util.CacheSize
import com.twitter.tormenta.spout.ScalaSpout

case class StormSer[T](injection: Injection[T, Array[Byte]]) extends Serialization[Storm, T]

object Storm {
  def source[T](spout: ScalaSpout[T])(implicit inj: Injection[T, Array[Byte]], timeOf: TimeExtractor[T]) =
    Producer.source[Storm, T, ScalaSpout[T]](spout)

  // TODO: Add an unapply that pulls the spout out of the source,
  // casting appropriately.
  implicit def ser[T](implicit inj: Injection[T, Array[Byte]]): Serialization[Storm, T] =
    StormSer(inj)
}

class StormOptions(opts: Map[Class[_], Any] = Map.empty) {
  def set(opt: SinkOption) = new StormOptions(opts + (opt.getClass -> opt))
  def set(opt: FlatMapOption) = new StormOptions(opts + (opt.getClass -> opt))
  def set(opt: CacheSize) = new StormOptions(opts + (opt.getClass -> opt))

  def get[T: Manifest]: Option[T] =
    opts.get(manifest[T].erasure).asInstanceOf[Option[T]]

  def getOrElse[T: Manifest](default: T): T =
    opts.getOrElse(manifest[T].erasure, default).asInstanceOf[T]
}

class Storm(jobName: String, options: Map[String, StormOptions]) extends Platform[Storm] {
  val SINK_ID = "sinkId"
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
    producer: Producer[Storm, T],
    suffix: String,
    id: Option[String])
    (implicit batcher: Batcher): List[String] = {
    producer match {
      case IdentityKeyedProducer(producer) => buildTopology(topoBuilder, producer, suffix, id)
      case NamedProducer(producer, newId) => buildTopology(topoBuilder, producer, suffix, Some(newId))
      case Source(source, ser, timeOf) => {
        val spoutName = "spout-" + suffix
        val spout = source.asInstanceOf[ScalaSpout[T]]
        val stormSpout = spout.getSpout { scheme =>
          scheme.map { t =>
            val batch = batcher.batchOf(new java.util.Date(timeOf(t)))
            (batch.id, t)
          }
        }
        topoBuilder.setSpout(spoutName, stormSpout, spout.parallelism)
        List(spoutName)
      }
      case FlatMappedProducer(producer, fn) => {
        val spoutNames = buildTopology(topoBuilder, producer, suffix, id)
        val boltName = FM_CONSTANT + suffix

        val bolt = if (isFinalFlatMap(suffix))
          new PairedBolt(fn.asInstanceOf[T => TraversableOnce[(_,_)]])
        else
          new FMBolt(fn)

        val parallelism = getOrElse(id, DEFAULT_FM_PARALLELISM)
        val declarer = topoBuilder.setBolt(boltName, bolt, parallelism.parHint)

        spoutNames.foreach { declarer.shuffleGrouping(_) }
        List(boltName)
      }
      case MergedProducer(l, r) => {
        val leftSuffix = "L-" + suffix
        val rightSuffix = "R-" + suffix

        buildTopology(topoBuilder, l, leftSuffix, id)
        buildTopology(topoBuilder, r, rightSuffix, id)
        List(leftSuffix, rightSuffix)
      }
      case LeftJoinedProducer(producer, svc) => sys.error("NOT!")
      case TeedProducer(l, streamSink) => sys.error("Low T!")
    }
  }

  /**
    * TODO: Completed is really still a producer. We can submit
    * topologies at the completed nodes, but otherwise they can
    * continue to flatMap, etc.
    */
  def populate[K, V](topologyBuilder: TopologyBuilder, builder: Completed[Storm, K, V]) = {
    implicit val batcher = builder.batcher
    val parents = buildTopology(topologyBuilder, builder.producer, END_SUFFIX, None)
    // TODO: Add wrapping case classes for memstore, etc, as in MemP.
    val supplier = builder.store.asInstanceOf[() => MergeableStore[(K, BatchID), V]]
    val bolt: IRichBolt = new SinkBolt(
      supplier,
      getOrElse(SINK_ID, DEFAULT_ONLINE_SUCCESS_HANDLER),
      getOrElse(SINK_ID, DEFAULT_ONLINE_EXCEPTION_HANDLER),
      getOrElse(SINK_ID, DEFAULT_SINK_CACHE),
      getOrElse(SINK_ID, DEFAULT_SINK_STORM_METRICS),
      getOrElse(SINK_ID, DEFAULT_MAX_WAITING_FUTURES),
      getOrElse(SINK_ID, IncludeSuccessHandler(false))
    )(builder.monoid)

    val parallelism = getOrElse(id, DEFAULT_SINK_PARALLELISM).parHint
    val declarer =
      topologyBuilder.setBolt(GROUP_BY_SUM, bolt, parallelism)

    parents.foreach { parentName =>
      declarer.fieldsGrouping(parentName, new Fields(AGG_KEY))
    }
    List(GROUP_BY_SUM)
  }

  /**
    * What's missing?
    *
    * - map-side aggregation in the final FlatMapBolt.
    * - metrics in the non-final FlatMapBolts.
    *
    * We can get these back by extending BaseBolt again.
    *
    * - leftJoin
    */
  def run[K, V](completed: Completed[Storm, K, V]): Unit = {
    val topologyBuilder = new TopologyBuilder
    populate(topologyBuilder, completed)
    StormSubmitter.submitTopology(
      "summingbird_" + jobName,
      new Config,
      topologyBuilder.createTopology
    )
  }
}
