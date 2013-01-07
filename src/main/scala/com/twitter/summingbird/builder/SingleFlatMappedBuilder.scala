package com.twitter.summingbird.builder

import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.BoltDeclarer
import backtype.storm.tuple.Fields
import cascading.flow.FlowDef
import com.twitter.algebird.Monoid
import com.twitter.chill.BijectionPair
import com.twitter.scalding.{ TypedPipe, TDsl, Dsl }
import com.twitter.summingbird.{ Constants, FlatMapper }
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.storm.{ StormEnv, FlatMapBolt }
import com.twitter.summingbird.util.CacheSize
import util.Random

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * SingleFlatMappedBuilder is the concrete implementation used
 * to transform an EventSource into a FlatMappedBuilder. These
 * guys can be fed into MergedFlatMappedBuilder instances
 * to combine with other event sources.
 */

class SingleFlatMappedBuilder[Event: Manifest,Time: Batcher: Manifest,Key: Manifest,Value: Manifest: Monoid]
(source: EventSource[Event,Time],
 override val keyOrdering: Ordering[Key],
 predOption: Option[(Event) => Boolean],
 flatMapper: FlatMapper[Event,Key,Value],
 flatMapParallelism: Int = 5,
 flatMapReducers: Int = 0,
 flatMapCacheSize: CacheSize = CacheSize(0))
extends FlatMappedBuilder[Time,Key,Value] {
  import Constants._

  // Register a BijectionPair for the time type and event type.
  override val eventCodecPairs = {
    val eventClass = manifest[Event].erasure.asInstanceOf[Class[Event]]
    List(BijectionPair(eventClass, source.eventCodec),
         BijectionPair(timeClass, source.timeCodec))
  }

  def flatMapName(suffix: String) = "flatMap" + suffix

  override def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String) {
    val spoutName = "spout" + suffix

    // TODO: After testing, try out pushing the flatMap up to here and
    // losing the flatMapBolt.
    val filteredSpout = source.spout(env)
      .getSpout { scheme =>
        predOption
          .map { scheme filter _ }
          .getOrElse { scheme }
          .map { e => (e, source.timeOf(e)) }
      }

    tb.setSpout(spoutName, filteredSpout, source.spoutParallelism(env))
    tb.setBolt(flatMapName(suffix), new FlatMapBolt(flatMapper, flatMapCacheSize), flatMapParallelism)
      .shuffleGrouping(spoutName)
  }

  override def attach(groupBySumBolt: BoltDeclarer, suffix: String) =
    groupBySumBolt.fieldsGrouping(flatMapName(suffix), new Fields(AGG_KEY))

  override def getFlatMappedPipe(batcher: Batcher[Time], lowerb: BatchID, env: ScaldingEnv)
  (implicit fd: FlowDef): TypedPipe[(Time,Key,Value)] = {
    import TDsl._
    import Dsl._

    val inputPipe = source.scaldingSource(batcher, lowerb, env)
    val filteredPipe = predOption.map { fn => inputPipe.filter { fn } }.getOrElse(inputPipe)
    val shardedPipe = {
      if (flatMapReducers <= 0)
        filteredPipe
      else
        // TODO: switch this to groupRandomly when it becomes available in scalding 0.8.2
        filteredPipe
          .groupBy { event => new Random().nextInt(flatMapReducers) }
          .mapValues { identity(_) } // hack to get scalding to actually do the groupBy
          .withReducers(flatMapReducers)
          .values
    }

    shardedPipe.flatMap { ev =>
      val t = source.timeOf(ev)
      // TODO: Scalding should accept TraversableOnce. Fix when
      // Scalding hits 0.9.0.
      flatMapper.encode(ev).toList.map { kv => (t, kv._1, kv._2) }
    }
  }

  // Set the number of processes assigned to this flatmapper in the
  // online flatmap step.
  def withParallelism(fmp: Int) =
    new SingleFlatMappedBuilder(source, keyOrdering, predOption, flatMapper,
                                fmp, flatMapReducers, flatMapCacheSize)

  // Set the number of reducers used to shard out the EventSource
  // flatmapper in the offline flatmap step.
  def withShards(fmr: Int) =
    new SingleFlatMappedBuilder(source, keyOrdering, predOption, flatMapper,
                                flatMapParallelism, fmr, flatMapCacheSize)

  // Set the cache size used in the online flatmap step.
  def withCacheSize(size: CacheSize) =
    new SingleFlatMappedBuilder(source, keyOrdering, predOption, flatMapper,
                                flatMapParallelism, flatMapReducers, size)
}
