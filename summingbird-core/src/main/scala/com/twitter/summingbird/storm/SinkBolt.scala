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
import backtype.storm.topology.{ IRichBolt, OutputFieldsDeclarer }
import backtype.storm.tuple.{ Fields, Tuple, Values }
import com.twitter.bijection.Injection
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.Constants
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.builder.{ OnlineExceptionHandler, OnlineSuccessHandler, SinkStormMetrics }
import com.twitter.storehaus.algebra.MergeableStore

import java.util.{ Map => JMap }

/**
 * The SinkBolt takes two related options: CacheSize and MaxWaitingFutures.
 * CacheSize sets the number of key-value pairs that the SinkBolt will accept
 * (and sum into an internal map) before committing out to the online store.
 *
 * To perform this commit, the SinkBolt iterates through the map of aggregated
 * kv pairs and performs a "+" on the store for each pair, sequencing these
 * "+" calls together using the Future monad. If the store has high latency,
 * these calls might take a bit of time to complete.
 *
 * MaxWaitingFutures(count) handles this problem by realizing a future
 * representing the "+" of kv-pair n only when kvpair n + 100 shows up in the bolt,
 * effectively pushing back against latency bumps in the host.
 *
 * The allowed latency before a future is forced is equal to
 * (MaxWaitingFutures * execute latency).
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class SinkBolt[Key, Value](
  storeSupplier: () => MergeableStore[(Key,BatchID), Value],
  @transient rpc: Injection[Option[Value], String],
  @transient successHandler: OnlineSuccessHandler,
  @transient exceptionHandler: OnlineExceptionHandler,
  metrics: SinkStormMetrics) extends BaseBolt(metrics.metrics) {
  import Constants._

  lazy val store = storeSupplier.apply

  val rpcInjection = MeatLocker(rpc)
  val exceptionHandlerBox = MeatLocker(exceptionHandler)
  val successHandlerBox = MeatLocker(successHandler)

  def executeDRPC(tuple: Tuple) {
    val key = tuple.getValue(0).asInstanceOf[Key]
    val batchID = tuple.getValue(1).asInstanceOf[BatchID]

    // This is just a read, we need the return info to do this properly
    val returnInfo = tuple.getString(2)

    // TODO: Add exceptionHandlerBox and successHandlerBox for RPC
    // calls.
    store.get((key, batchID))
      .map { rpcInjection.get(_) }
      .onSuccess { optVStr =>
      onCollector { _.emit(RPC_STREAM, new Values(optVStr, returnInfo)) }
    }
  }

  // TODO: Think about how we could compose bolts using an implicit
  // Injection[T,Tuple] This is really just the invert function.  The
  // problem is that Storm emits Values and receives Tuples.
  def unpack(tuple: Tuple) = {
    val batchID = tuple.getValue(0).asInstanceOf[BatchID]
    val key = tuple.getValue(1).asInstanceOf[Key]
    val value = tuple.getValue(2).asInstanceOf[Value]
    ((key, batchID), value)
  }

  override def execute(tuple: Tuple) {
    tuple.getSourceComponent match {
      case DRPC_DECODER => executeDRPC(tuple)
      case _ => store.merge(unpack(tuple))
          .onSuccess { _ => successHandlerBox.get.handlerFn() }
          .handle(exceptionHandlerBox.get.handlerFn)
    }
    onCollector { _.ack(tuple) }
  }

  override def declareOutputFields(dec : OutputFieldsDeclarer) {
    dec.declareStream(RPC_STREAM, new Fields(VALUE_FIELD, RETURN_INFO))
  }

  override def cleanup { store.close }
}
