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

import com.twitter.algebird.{ Monoid, SummingQueue }
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.storm.option.{ CacheSize, FlatMapStormMetrics }
import com.twitter.storehaus.algebra.MergeableStore

import MergeableStore.enrich

import java.util.{ Date, Map => JMap }

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class FinalFlatMapBolt[Event, Key, Value](
  flatMapOp: FlatMapOperation[Event, (Key, Value)],
  cacheSize: CacheSize,
  metrics: FlatMapStormMetrics)
  (implicit monoid: Monoid[Value], batcher: Batcher)
    extends BaseBolt(metrics.metrics) {
  var collectorMergeable: MergeableStore[(Key, BatchID), Value] = null

  override val fields = {
    import Constants._
    Some(new Fields(AGG_BATCH, AGG_KEY, AGG_VALUE))
  }

  override def prepare(
    conf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    super.prepare(conf, context, oc)
    onCollector { collector =>
      collectorMergeable =
        new CollectorMergeableStore[Key, Value](collector)
          .withSummer { m =>
          implicit val monoid: Monoid[Value] = m
          SummingQueue(cacheSize.size.getOrElse(0))
        }
    }
  }

  override def execute(tuple: Tuple) {
    val (time, event) = tuple.getValue(0).asInstanceOf[(Long, Event)]
    val batchID = batcher.batchOf(new Date(time))

    /**
      * the flatMap function returns a future.
      *
      *  each resulting key value pair is merged into the output once
      * the future completes the input tuple is acked once the future
      * completes.
      */
    flatMapOp.apply(event).foreach { pairs =>
      pairs.foreach { case (k, v) =>
        onCollector { _ => collectorMergeable.merge((k, batchID) -> v) }
      }
      ack(tuple)
    }
  }

  override def cleanup { flatMapOp.close }
}
