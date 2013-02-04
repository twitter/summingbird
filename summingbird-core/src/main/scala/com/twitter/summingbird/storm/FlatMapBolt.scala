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

package com.twitter.summingbird.storm

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }

import com.twitter.algebird.Monoid
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.Constants
import com.twitter.summingbird.util.CacheSize
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.builder.FlatMapStormMetrics

import java.util.{ Map => JMap }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class FlatMapBolt[Event,Time,Key,Value](@transient fm: FlatMapOperation[Event,Key,Value],
                                        cacheSize: CacheSize,
                                        metrics: FlatMapStormMetrics)
(implicit monoid: Monoid[Value], batcher: Batcher[Time])
extends BufferingBolt[Map[(Key, BatchID), Value]](cacheSize) {
  val flatMapBox = MeatLocker(fm)

  // TODO: Once this issue's fixed
  // (https://github.com/twitter/tormenta/issues/1), emit scala
  // tuples directly.

  protected def emitMap(m: Map[(Key,BatchID),Value]) {
    toCollector { coll =>
      m foreach { case ((k,batchID),v) =>
        coll.emit(new Values(batchID.asInstanceOf[AnyRef], // BatchID
                             k.asInstanceOf[AnyRef], // Key
                             v.asInstanceOf[AnyRef])) // Value
      }
    }
  }

  override def prepare(conf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    super.prepare(conf, context, oc)
    metrics.metrics().foreach { _.register(context) }
  }

  def cachedExecute(pairs: TraversableOnce[(Key,Value)], batchID: BatchID) {
    val pairMaps: TraversableOnce[Map[(Key,BatchID),Value]] =
      pairs flatMap { case (k,v) => buffer( Map((k, batchID) -> v) ) }

    emitMap(Monoid.sum(pairMaps))
  }

  def uncachedExecute(pairs: TraversableOnce[(Key,Value)], batchID: BatchID) {
    // TODO: Because the bolt has persistent state, we can have the
    // bolt sample the stream and evaluate if this foldLeft is needed
    // (and remove it if unnecessary).
    val toEmit =
      pairs.foldLeft(Monoid.zero[Map[(Key, BatchID), Value]]) { (acc, pair) =>
        val (k,v) = pair
        Monoid.plus(acc, Map((k,batchID) -> v))
      }
    emitMap(toEmit)
  }

  override def execute(tuple: Tuple) {
    // TODO: We need to get types down into the IRichSpout so we can validate this.
    val (event,time) = tuple.getValue(0).asInstanceOf[(Event,Time)]
    val batchID = batcher.batchOf(time)
    flatMapBox.get.apply(event) foreach { pairs =>
      if (cacheCount.isDefined)
        cachedExecute(pairs, batchID)
      else
        uncachedExecute(pairs, batchID)

      // TODO: Think about whether we want to ack tuples when they're
      // actually processed, vs always acking them here. This will
      // complicate the logic of the cached case (as we'll have to track
      // the tuple -> kv mapping.
      collector.ack(tuple)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    import Constants._
    declarer.declare(new Fields(AGG_BATCH, AGG_KEY, AGG_VALUE))
  }

  override def cleanup { flatMapBox.get.close }
  override val getComponentConfiguration = null
}
