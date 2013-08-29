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

import backtype.storm.tuple.{Fields, Tuple, Values}
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.Constants._
import com.twitter.summingbird.storm.option.FlatMapStormMetrics

/**
  * This bolt is used for intermediate flatMapping before a grouping.
  * Its output storm-tuple contains a single scala.Tuple2[Long, U] at
  * the 0 position. The Long is the timestamp of the source object
  * from which U was derived. Each U is one of the output items of the
  * flatMapOp.
  */
class IntermediateFlatMapBolt[T](
  flatMapOp: FlatMapOperation[T, _],
  metrics: FlatMapStormMetrics) extends BaseBolt(metrics.metrics) {

  def toValues(time: Long, item: Any): Values = new Values((time, item))

  override val fields = Some(new Fields("pair"))

  override def execute(tuple: Tuple) {
    val (time, t) = tuple.getValue(0).asInstanceOf[(Long, T)]

    flatMapOp(t).foreach { items =>
      items.foreach { u =>
        onCollector(_.emit(toValues(time, u)))
      }
      ack(tuple)
    }
  }

  override def cleanup { flatMapOp.close }
}
