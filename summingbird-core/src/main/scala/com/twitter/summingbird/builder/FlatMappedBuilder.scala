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
import com.twitter.bijection.Bijection
import com.twitter.chill.BijectionPair
import com.twitter.scalding.{TypedPipe, Mode}
import com.twitter.storehaus.Store
import com.twitter.summingbird.{Env, FlatMapper, FunctionFlatMapper}
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.store.CompoundStore
import com.twitter.summingbird.storm.StormEnv
import com.twitter.summingbird.service.CompoundService

import java.io.Serializable

/**
 * The flatmapped builder represents an EventSource with a FlatMapper attached.
 * A FlatMappedBuilder can be added to other flatmapped builders with the same
 * key, value and time types.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

abstract class FlatMappedBuilder[Time, Key, Value] extends Serializable {
  // Abstract methods that must be overridden are listed first

  // Sources of many event types might create the same key-value pairs, so we use a List.
  // TODO: eventCodecPairs should probably come from SourceBuilders
  val eventCodecPairs: List[BijectionPair[_]]
  val sourceBuilders: List[SourceBuilder[_, Time]]

  def flatMapBuilder[Key2,Val2](newFlatMapper: FlatMapper[(Key,Value),Key2,Val2])
    : FlatMappedBuilder[Time, Key2, Val2]

  def leftJoin[JoinedValue](service: CompoundService[Key, JoinedValue]):
    FlatMappedBuilder[Time,Key,(Value, Option[JoinedValue])]

  def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String): Unit
  def attach(groupBySumBolt: BoltDeclarer, suffix: String): BoltDeclarer
  def getFlatMappedPipe(batcher: Batcher[Time], lowerb: BatchID, env: ScaldingEnv)
    (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time,Key,Value)]

  // Methods that have defaults. It may still be more efficient to override:

  def flatMap[Key2, Value2](fn: ((Key,Value)) => TraversableOnce[(Key2, Value2)]):
    FlatMappedBuilder[Time,Key2,Value2] = flatMapBuilder(new FunctionFlatMapper(fn))

  def filter(fn: ((Key,Value)) => Boolean): FlatMappedBuilder[Time,Key,Value] =
    flatMap[Key,Value] { tup => if(fn(tup)) Some(tup) else None }

  def map[Key2, Value2](fn: ((Key,Value)) => (Key2,Value2)): FlatMappedBuilder[Time,Key2,Value2] =
    flatMap[Key2,Value2] { tup => Iterable(fn(tup)) }

  // This allows you to build a storm topology / scalding job
  def groupAndSumTo[StoreType <: Store[StoreType, (Key, BatchID), Value]](store: CompoundStore[StoreType, Key, Value])
  (implicit env: Env,
   keyMf: Manifest[Key],
   valMf: Manifest[Value],
   keyCodec: Bijection[Key,Array[Byte]],
   valCodec: Bijection[Value,Array[Byte]],
   batcher: Batcher[Time],
   monoid: Monoid[Value],
   keyOrdering: Ordering[Key]): CompletedBuilder[StoreType,Time,Key,Value] = {
    val cb = new CompletedBuilder(this, store, keyCodec, valCodec)
    env.builder = cb
    cb
  }

  // useful when you need to merge two different Event sources
  def ++(other: FlatMappedBuilder[Time,Key,Value]): FlatMappedBuilder[Time,Key,Value] =
    new MergedFlatMappedBuilder(this, other)
}
