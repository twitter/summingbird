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

import backtype.storm.topology.base._
import backtype.storm.topology._
import backtype.storm.task.TopologyContext
import backtype.storm.tuple._
import com.twitter.bijection.Injection
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.Constants
import com.twitter.summingbird.batch.BatchID

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// RPC keys come in as strings. This bolt decodes that string into a
// Key, which is then routed to the sink for proper fulfillment of the
// RPC call.
class DecoderBolt[Key](@transient rpc: Injection[(Key, BatchID), String]) extends BaseBasicBolt {
  import Constants._

  val rpcBijection = MeatLocker(rpc)

  override def execute(tuple: Tuple, collector: BasicOutputCollector) {
    val (key, batchID) = rpcBijection.get.invert(tuple.getString(0)).get // TODO better error handling here
    val returnInfo = tuple.getString(1)

    collector.emit(new Values(key.asInstanceOf[AnyRef],
                              batchID.asInstanceOf[AnyRef],
                              returnInfo))
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
    declarer.declare(new Fields(AGG_KEY, AGG_BATCH, RETURN_INFO))
  }
}
