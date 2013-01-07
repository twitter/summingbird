package com.twitter.summingbird.storm

import backtype.storm.topology.base._
import backtype.storm.topology._
import backtype.storm.task.TopologyContext
import backtype.storm.tuple._

import com.twitter.bijection.Bijection
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.Constants
import com.twitter.summingbird.sink.CompoundSink
import com.twitter.summingbird.batch.BatchID


/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class SinkBolt[Key,Value](@transient sink: CompoundSink[_,Key,Value],
                          @transient rpc: Bijection[Value,String]) extends BaseBasicBolt {
  import Constants._

  // A trick to avoid using Java serialization on these things:
  val sinkBox = MeatLocker(sink)
  val rpcBijection = MeatLocker(rpc)

  override def execute(tuple: Tuple, collector: BasicOutputCollector) {
    val thisSink  = sinkBox.get

    if(tuple.getSourceComponent == DRPC_DECODER) {
      val key = tuple.getValue(0).asInstanceOf[Key]

      // TODO: Map the future here that submits its value to Storm's
      // RPC mechanism and returns Future.Unit immediately.
      val retVal = thisSink.get(key).map { v =>
        rpcBijection.get.apply(v getOrElse sinkBox.get.monoid.zero)
      }.apply

      // This is just a read, we need the return info to do this properly
      val returnInfo = tuple.getString(1)

      collector.emit(RPC_STREAM, new Values(retVal, returnInfo))
    }
    else {
      val batchID = tuple.getValue(0).asInstanceOf[BatchID]
      val key = tuple.getValue(1).asInstanceOf[Key]
      val value = tuple.getValue(2).asInstanceOf[Value]

      thisSink.increment(((key,batchID), value)).apply
    }
  }

  override def declareOutputFields(dec : OutputFieldsDeclarer) {
    dec.declareStream(RPC_STREAM, new Fields(VALUE_FIELD, RETURN_INFO))
  }
}
