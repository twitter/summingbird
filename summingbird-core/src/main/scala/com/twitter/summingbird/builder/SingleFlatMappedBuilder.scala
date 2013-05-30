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

package com.twitter.summingbird.builder

import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.BoltDeclarer
import backtype.storm.tuple.Fields
import cascading.flow.FlowDef
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ TypedPipe, TDsl, Dsl, Mode }
import com.twitter.summingbird.{ Constants, FlatMapper }
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.scalding.{ScaldingEnv, FlatMapOperation => ScaldingFlatMap }
import com.twitter.summingbird.service.CompoundService
import com.twitter.summingbird.storm.{ StormEnv, FinalFlatMapBolt, FlatMapOperation => StormFlatMap }
import com.twitter.summingbird.util.CacheSize
import scala.util.Random

/**
 * SingleFlatMappedBuilder is the abstract class used to transform an
 * EventSource into a FlatMappedBuilder. These guys can be fed into
 * MergedFlatMappedBuilder instances to combine with other event
 * sources.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class SingleFlatMappedBuilder[Event,Key,Value](
  sourceBuilder: SourceBuilder[Event],
  stormFm: StormFlatMap[Event, (Key, Value)],
  scaldingFm: ScaldingFlatMap[Event, Key, Value],
  flatMapParallelism: FlatMapParallelism = Constants.DEFAULT_FM_PARALLELISM,
  flatMapCacheSize: CacheSize = Constants.DEFAULT_FM_CACHE,
  stormMetrics: FlatMapStormMetrics = Constants.DEFAULT_FM_STORM_METRICS)
    extends FlatMappedBuilder[Key,Value] {
  import Constants._

  /** Use this with named params for easy copying.
   */
  def copy[K2,V2](storm: StormFlatMap[Event, (K2, V2)],
    scalding: ScaldingFlatMap[Event, K2, V2],
    parallelism: FlatMapParallelism = flatMapParallelism,
    cacheSize: CacheSize = flatMapCacheSize,
    stormMetrics: FlatMapStormMetrics = stormMetrics,
    source: SourceBuilder[Event] = sourceBuilder):
    SingleFlatMappedBuilder[Event, K2, V2] =
      new SingleFlatMappedBuilder(source, storm, scalding, parallelism, cacheSize, stormMetrics)

  override val sourceBuilders = List(sourceBuilder)

  def flatMapName(suffix: String) = "flatMap" + suffix

  override def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String) {
    val spoutName = sourceBuilder.addToTopo(env, tb, suffix)
    // The monoid here must be of the right type or we are screwed anyway, the builder
    // ensures this:
    implicit val monoid: Monoid[Value] = env.builder.monoid.asInstanceOf[Monoid[Value]]
    implicit val batcher: Batcher = env.builder.batcher
    tb.setBolt(flatMapName(suffix),
               new FinalFlatMapBolt(stormFm, flatMapCacheSize, stormMetrics),
               flatMapParallelism.parHint)
      .shuffleGrouping(spoutName)
  }

  override def attach(groupBySumBolt: BoltDeclarer, suffix: String) =
    groupBySumBolt.fieldsGrouping(flatMapName(suffix), new Fields(AGG_KEY))

  override def flatMapBuilder[Key2, Val2](newFlatMapper: FlatMapper[(Key,Value), (Key2,Val2)]): FlatMappedBuilder[Key2, Val2] = {

    val newStorm = stormFm.andThen(StormFlatMap(newFlatMapper))
    val newScalding = scaldingFm.andThen(ScaldingFlatMap(newFlatMapper))
    copy(newStorm, newScalding)
  }

  override def getFlatMappedPipe(batcher: Batcher, lowerb: BatchID, env: ScaldingEnv)
  (implicit fd: FlowDef, mode: Mode): TypedPipe[(Long,Key,Value)] = {
    val src = sourceBuilder.getFlatMappedPipe(batcher, lowerb, env)
    scaldingFm(src)(fd, mode, env).map { case (t, (k,v)) => (t,k,v) }
  }

  def leftJoin[JoinedValue](service: CompoundService[Key, JoinedValue])
      : FlatMappedBuilder[Key,(Value, Option[JoinedValue])] = {
    val newStorm = StormFlatMap.combine(stormFm, service.online)
    val newScalding = ScaldingFlatMap.combine(scaldingFm, service.offline)
    copy(newStorm, newScalding)
  }

  // Set the cache size used in the online flatmap step.
  def set(size: CacheSize) = copy(stormFm, scaldingFm, cacheSize = size)

  def set(opt: FlatMapOption) =
    opt match {
      // Set the number of processes assigned to this flatmapper in the
      // online flatmap step.
      case fmp: FlatMapParallelism => copy(stormFm, scaldingFm, parallelism = fmp)
      case fsh: FlatMapShards => copy(stormFm, scaldingFm, source = sourceBuilder.set(fsh))
      case metrics: FlatMapStormMetrics => copy(stormFm, scaldingFm, stormMetrics = metrics)
    }
}
