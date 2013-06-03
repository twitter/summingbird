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

import com.twitter.scalding.{TypedPipe, Mode}

import com.twitter.summingbird.FlatMapper
import com.twitter.summingbird.batch.{ Batcher, BatchID }

import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.service.CompoundService
import com.twitter.summingbird.sink.CompoundSink
import com.twitter.summingbird.storm.StormEnv

/**
 * MergedFlatMappedBuilder describes the union of two individual
 * FlatMappedBuilders.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class MergedFlatMappedBuilder[Key,Value](left: FlatMappedBuilder[Key,Value], right: FlatMappedBuilder[Key,Value])
    extends FlatMappedBuilder[Key,Value] {

  override lazy val sourceBuilders = left.sourceBuilders ++ right.sourceBuilders

  override def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String) {
    left.addToTopo(env, tb, suffix + "-L")
    right.addToTopo(env, tb, suffix + "-R")
  }

  override def attach(groupBySumBolt: BoltDeclarer, suffix: String) = {
    val ldec = left.attach(groupBySumBolt, suffix + "-L")
    right.attach(ldec, suffix + "-R")
  }

  override def flatMapBuilder[Key2,Val2](newFlatMapper: FlatMapper[(Key, Value), (Key2, Val2)]): FlatMappedBuilder[Key2, Val2] =
    new MergedFlatMappedBuilder(left.flatMapBuilder(newFlatMapper),
      right.flatMapBuilder(newFlatMapper))

  override def filter(fn: ((Key,Value)) => Boolean) =
    new MergedFlatMappedBuilder(left.filter(fn), right.filter(fn))

  override def getFlatMappedPipe(batcher: Batcher, lowerb: BatchID, env: ScaldingEnv)
    (implicit fd : FlowDef, mode: Mode): TypedPipe[(Long,Key,Value)] =
    left.getFlatMappedPipe(batcher, lowerb, env) ++ right.getFlatMappedPipe(batcher, lowerb, env)

  override def leftJoin[JoinedValue](service: CompoundService[Key, JoinedValue]) =
    new MergedFlatMappedBuilder(left.leftJoin(service), right.leftJoin(service))

  def write[Written](sink: CompoundSink[Written])(conversion: ((Key, Value)) => TraversableOnce[Written]):
      FlatMappedBuilder[Key, Value] =
    new MergedFlatMappedBuilder(left.write(sink)(conversion), right.write(sink)(conversion))
}
