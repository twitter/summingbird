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

import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Fields, Tuple, Values}
import com.twitter.storehaus.algebra.MergeableStore.enrich

import com.twitter.summingbird.Constants._

/**
  * building flatMapBolt back up from the beginning.
  *
  * @author Oscar Boykin
  * @author Sam Ritchie
  * @author Ashu Singhal
  */

class FMBolt[T](fn: T => TraversableOnce[Any]) extends BaseBasicBolt {
  def toValues(id: Long, item: Any): Values =
    new Values((id, item))

  def fields: Fields = new Fields("pair")

  override def execute(tuple: Tuple, collector: BasicOutputCollector) {
    val (batch, t) = tuple.getValue(0).asInstanceOf[(Long, T)]
    fn(t).foreach { item =>
      collector.emit(toValues(batch, item))
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(fields)
  }
}

class PairedBolt[T](fn: T => TraversableOnce[(Any, Any)]) extends FMBolt[T](fn) {
  override def toValues(id: Long, pair: (Any, Any)) = {
    val (k, v) = pair
    new Values(
      id.asInstanceOf[AnyRef],
      k.asInstanceOf[AnyRef],
      v.asInstanceOf[AnyRef]
    )
  }

  override val fields = new Fields(AGG_BATCH, AGG_KEY, AGG_VALUE)
}
