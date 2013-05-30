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

import cascading.flow.FlowDef

import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.scalding.{TypedPipe, Mode}

import com.twitter.summingbird.batch.{Batcher, BatchID}
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.{ Constants, FlatMapper, FunctionFlatMapper }

import com.twitter.summingbird.storm.{FlatMapOperation => StormFlatMap, StormEnv}
import com.twitter.summingbird.scalding.{FlatMapOperation => ScaldingFlatMap, ScaldingEnv}

import java.io.Serializable
import java.util.Date

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// The SourceBuilder is the first level of the expansion.

class SourceBuilder[Event](
  val eventSource: EventSource[Event],
  timeOf: Event => Date,
  predOption: Option[(Event) => Boolean] = None,
  flatMapShards: FlatMapShards = Constants.DEFAULT_FM_SHARDS)
  (implicit eventMf: Manifest[Event], eventCodec: Injection[Event, Array[Byte]])
    extends Serializable {
  import Constants._

  val eventCodecPair = CompletedBuilder.injectionPair[Event](eventCodec)

  def filter(newPred: (Event) => Boolean): SourceBuilder[Event] = {
    val newPredicate =
      predOption.map { old => { (e: Event) => old(e) && newPred(e) } }
        .orElse(Some(newPred))

    new SourceBuilder(eventSource, timeOf, newPredicate)
  }

  def map[Key: Manifest, Val: Manifest](fn: (Event) => (Key,Val)) =
    flatMap[Key, Val] { e : Event => Some(fn(e)) }

  def flatMap[Key: Manifest, Val: Manifest]
  (fn : (Event) => TraversableOnce[(Key,Val)])  =
    flatMapBuilder[Key, Val](new FunctionFlatMapper[Event, (Key, Val)](fn))

  def flatMapBuilder[Key: Manifest, Val: Manifest](newFlatMapper: FlatMapper[Event, (Key, Val)])
      : SingleFlatMappedBuilder[Event, Key, Val] = {
    val storm = StormFlatMap[Event, (Key, Val)](newFlatMapper)
    val scalding = ScaldingFlatMap[Event, Key, Val](newFlatMapper)
    new SingleFlatMappedBuilder[Event, Key, Val](this, storm, scalding)
  }

  // Here are the planning methods:
  def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String): String = {
    val spoutName = "spout" + suffix

    // TODO: After testing, try out pushing the flatMap up to here and
    // losing the flatMapBolt.
    val scalaSpout = eventSource.spout.get

    /**
      * the filteredSpout emits pairs of (Event, Date). Kryo handles
      * Date natively by only serializing the Long, so ths is just as
      * efficient as extracting the millis. FlatMapBolt needs the
      * java.util.Date to tag each event with a BatchID, so we keep the
      * date around.
      *
      * TODO: Would it make more sense to assign a BatchID here?
      */
    val filteredSpout = scalaSpout
      .getSpout { scheme =>
        predOption
          .map { scheme filter _ }
          .getOrElse { scheme }
          .map { e => (e, timeOf(e)) }
      }

    tb.setSpout(spoutName, filteredSpout, scalaSpout.parallelism)
    spoutName
  }

  def getFlatMappedPipe(batcher: Batcher, lowerb: BatchID, env: ScaldingEnv)
    (implicit fd: FlowDef, mode: Mode): TypedPipe[(Long, Event)] = {
    //import TDsl._
    //import Dsl._

    val inputPipe = eventSource.offline.get.scaldingSource(batcher, lowerb, env)(timeOf)
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
    shardedPipe.map { ev => (timeOf(ev).getTime, ev) }
  }

  // Set the number of reducers used to shard out the EventSource
  // flatmapper in the offline flatmap step.
  def set(fms: FlatMapShards): SourceBuilder[Event] =
    new SourceBuilder(eventSource, timeOf, predOption, fms)
}
