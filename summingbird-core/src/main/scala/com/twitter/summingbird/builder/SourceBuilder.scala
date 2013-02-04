/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.builder

import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.BoltDeclarer

import cascading.flow.FlowDef

import com.twitter.algebird.Monoid
import com.twitter.chill.BijectionPair
import com.twitter.scalding.{TypedPipe, Mode}

import com.twitter.summingbird.batch.{Batcher, BatchID}
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.{ Constants, FlatMapper, FunctionFlatMapper }

import com.twitter.summingbird.storm.{FlatMapOperation => StormFlatMap, StormEnv}
import com.twitter.summingbird.scalding.{FlatMapOperation => ScaldingFlatMap, ScaldingEnv}

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// The SourceBuilder is the first level of the expansion.

class SourceBuilder[Event: Manifest, Time : Batcher: Manifest]
(val eventSource: EventSource[Event,Time],
 predOption: Option[(Event) => Boolean] = None,
 flatMapShards: FlatMapShards = Constants.DEFAULT_FM_SHARDS
 )
extends java.io.Serializable {
  import Constants._

  // Register a BijectionPair for the time type and event type.
  val eventCodecPairs = {
    val eventClass = manifest[Event].erasure.asInstanceOf[Class[Event]]
    val timeClass = manifest[Time].erasure.asInstanceOf[Class[Time]]
    List(BijectionPair(eventClass, eventSource.eventCodec),
         BijectionPair(timeClass, eventSource.timeCodec))
  }

  def filter(newPred: (Event) => Boolean): SourceBuilder[Event,Time] = {
    val newPredicate =
      predOption.map { old => { (e: Event) => old(e) && newPred(e) } }
        .orElse(Some(newPred))

    new SourceBuilder(eventSource, newPredicate)
  }

  def map[Key: Manifest, Val: Manifest](fn: (Event) => (Key,Val)) =
    flatMap {e : Event => Some(fn(e)) }

  def flatMap[Key: Manifest, Val: Manifest]
  (fn : (Event) => TraversableOnce[(Key,Val)])  =
    flatMapBuilder(new FunctionFlatMapper(fn))

  def flatMapBuilder[Key: Manifest, Val: Manifest](newFlatMapper: FlatMapper[Event,Key,Val])
    : SingleFlatMappedBuilder[Event,Time,Key,Val] = {
    val storm = StormFlatMap(newFlatMapper)
    val scalding = ScaldingFlatMap(newFlatMapper)
    new SingleFlatMappedBuilder[Event,Time,Key,Val](this, storm, scalding)
  }

  // Here are the planning methods:
  def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String): String = {
    val spoutName = "spout" + suffix

    // TODO: After testing, try out pushing the flatMap up to here and
    // losing the flatMapBolt.
    val filteredSpout = eventSource.spout(env)
      .getSpout { scheme =>
        predOption
          .map { scheme filter _ }
          .getOrElse { scheme }
          .map { e => (e, eventSource.timeOf(e)) }
      }

    tb.setSpout(spoutName, filteredSpout, eventSource.spoutParallelism(env))
    spoutName
  }

  def getFlatMappedPipe(batcher: Batcher[Time], lowerb: BatchID, env: ScaldingEnv)
  (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time, Event)] = {
    //import TDsl._
    //import Dsl._

    val inputPipe = eventSource.scaldingSource(batcher, lowerb, env)
    val filteredPipe = predOption.map { fn => inputPipe.filter { fn } }
      .getOrElse(inputPipe)

    // Sharding is a kind of parallelism on the source done to get around Hadoop issues:
    val shards = flatMapShards.count
    val shardedPipe = {
      if (shards <= 1)
        filteredPipe
      else
        // TODO: switch this to groupRandomly when it becomes
        // available in the typed API
        filteredPipe
          .groupBy { event => new java.util.Random().nextInt(shards) }
          .mapValues { identity(_) } // hack to get scalding to actually do the groupBy
          .withReducers(shards)
          .values
    }
    shardedPipe.map { ev => (eventSource.timeOf(ev), ev) }
  }

  // Set the number of reducers used to shard out the EventSource
  // flatmapper in the offline flatmap step.
  def set(fms: FlatMapShards): SourceBuilder[Event, Time] =
    new SourceBuilder(eventSource, predOption, fms)
}
