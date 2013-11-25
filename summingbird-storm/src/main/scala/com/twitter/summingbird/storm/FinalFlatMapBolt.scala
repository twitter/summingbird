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

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }

import com.twitter.algebird.{ SummingQueue, Semigroup, MapAlgebra }
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.batch.{ Batcher, BatchID, Timestamp}
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.storm.option.{
  AnchorTuples,
  FlatMapStormMetrics,
  MaxWaitingFutures,
  MaxFutureWaitTime
}
import com.twitter.util.{Return, Throw}
import com.twitter.storehaus.algebra.SummerConstructor
import com.twitter.summingbird.option.CacheSize
import com.twitter.storehaus.algebra.MergeableStore

import com.twitter.util.{Future}

import MergeableStore.enrich

import java.util.{ Date, Arrays => JArrays, List => JList, Map => JMap }

import scala.collection.JavaConverters._
import scala.collection.breakOut

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */
class FinalFlatMapBolt[Event, Key, Value](
  @transient flatMapOp: FlatMapOperation[Event, (Key, Value)],
  cacheSize: CacheSize,
  metrics: FlatMapStormMetrics,
  anchor: AnchorTuples,
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime)
  (implicit monoid: Semigroup[Value], batcher: Batcher)
    extends AsyncBaseBolt[Event, ((Key, BatchID), Value)](metrics.metrics,
                                                          anchor,
                                                          maxWaitingFutures,
                                                          maxWaitingTime,
                                                          true) {

  import JListSemigroup._
  import Constants._

  type KBV = ((Key, BatchID), Value)

  val lockedOp = Externalizer(flatMapOp)
  val squeue: SummingQueue[Map[(Key, BatchID), (JList[Tuple], Timestamp, Value)]] =
    SummingQueue(cacheSize.size.getOrElse(0))

  override val decoder = new SingleItemInjection[Event](VALUE_FIELD)
  override val encoder = new KeyValueInjection[(Key, BatchID), Value](AGG_KEY, AGG_VALUE)

  def cache(tuple: Tuple,
            time: Timestamp,
            items: TraversableOnce[(Key, Value)]): Iterable[(JList[Tuple], Future[TraversableOnce[(Timestamp, KBV)]])] = {

    val batchID = batcher.batchOf(time)
    squeue(MapAlgebra.sumByKey(items.map { case (k, v) => (k, batchID) -> (lift(tuple), time, v) }))
      .map { kvs =>
          kvs.iterator
            .map { case ((k, b), (tups, ts, v)) =>
              (tups, Future.value(List((ts, ((k,b), v)))))
            }
            .toList // don't need the ordering, so we could save by pushing a stack or something
      }
      .getOrElse(Nil)
  }

  override def apply(tup: Tuple,
                     timeIn: (Timestamp, Event)) =
    lockedOp.get.apply(timeIn._2).map { cache(tup, timeIn._1, _) }

  override def cleanup { lockedOp.get.close }
}
