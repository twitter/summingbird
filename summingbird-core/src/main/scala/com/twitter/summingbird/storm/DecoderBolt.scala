package com.twitter.summingbird.storm

import backtype.storm.topology.base._
import backtype.storm.topology._
import backtype.storm.task.TopologyContext
import backtype.storm.tuple._
import com.twitter.bijection.Bijection
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.Constants
import com.twitter.summingbird.batch.BatchID

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// RPC keys come in as strings. This bolt decodes that string into a
// Key, which is then routed to the sink for proper fulfillment of the
// RPC call.
class DecoderBolt[Key](@transient rpc: Bijection[(Key, BatchID), String]) extends BaseBasicBolt {
  import Constants._

  val rpcBijection = MeatLocker(rpc)

  override def execute(tuple: Tuple, collector: BasicOutputCollector) {
    val (key, batchID) = rpcBijection.get.invert(tuple.getString(0))
    val returnInfo = tuple.getString(1)

    collector.emit(new Values(key.asInstanceOf[AnyRef],
                              batchID.asInstanceOf[AnyRef],
                              returnInfo))
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
    declarer.declare(new Fields(AGG_KEY, AGG_BATCH, RETURN_INFO))
  }
}
