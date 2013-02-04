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
import com.twitter.bijection.Bijection
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.Constants
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.builder.{
  MaxWaitingFutures,
  OnlineExceptionHandler,
  OnlineSuccessHandler,
  SinkStormMetrics
}
import com.twitter.storehaus.Store
import com.twitter.summingbird.util.{ CacheSize, FutureQueue }
import com.twitter.util.Future

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
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class SinkBolt[StoreType <: Store[StoreType, (Key,BatchID), Value], Key, Value: Monoid]
(@transient store: StoreType,
 @transient rpc: Bijection[Option[Value], String],
 @transient successHandler: OnlineSuccessHandler,
 @transient exceptionHandler: OnlineExceptionHandler,
 cacheSize: CacheSize,
 maxWaitingFutures: MaxWaitingFutures,
 metrics: SinkStormMetrics)
extends BufferingBolt[Map[(Key, BatchID), Value]](cacheSize) {
  import Constants._

  val storeBox = MeatLocker(store)

  // This FutureQueue maintains some number of potentially realized
  // futures -- once the queue fills up, every addition with += calls
  // "apply" on the future and drops it off of the queue. This logic
  // allows the SinkBolt to push back against a store that's going
  // particularly slow. For stores that respond very quickly to "+"
  // calls, by the time "apply" is called on the Future[StoreType] the
  // request will already have been completed.
  var futureQueue: FutureQueue[StoreType] = null

  val rpcBijection = MeatLocker(rpc)
  val exceptionHandlerBox = MeatLocker(exceptionHandler)
  val successHandlerBox = MeatLocker(successHandler)

  override def prepare(conf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    super.prepare(conf, context, oc)
    futureQueue = FutureQueue[StoreType](Future.value(storeBox.get), maxWaitingFutures.get)
    metrics.metrics().foreach { _.register(context) }
  }

  def executeDRPC(tuple: Tuple) {
    val key = tuple.getValue(0).asInstanceOf[Key]
    val batchID = tuple.getValue(1).asInstanceOf[BatchID]

    // This is just a read, we need the return info to do this properly
    val returnInfo = tuple.getString(2)

    futureQueue.last foreach { store =>
      store.get((key, batchID))
        .map { rpcBijection.get(_) }
        .onSuccess { optVStr =>
          // TODO: Add exceptionHandlerBox and successHandlerBox for RPC
          // calls.
          collector.emit(RPC_STREAM, new Values(optVStr, returnInfo))
        }
    }
  }

  // TODO: Think about how we could compose bolts using an implicit
  // Bijection[T,Tuple] This is really just the invert function.
  // The problem is that Storm emits Values and receives Tuples.
  def unpack(tuple: Tuple) = {
    val batchID = tuple.getValue(0).asInstanceOf[BatchID]
    val key = tuple.getValue(1).asInstanceOf[Key]
    val value = tuple.getValue(2).asInstanceOf[Value]
    ((key, batchID), value)
  }

  // TODO: Remove these next two functions once AggregatingStore is merged:
  // https://github.com/twitter/storehaus/pull/16

  protected def increment(store: StoreType, item: ((Key,BatchID), Value)): Future[StoreType] = {
    val (k,v) = item
    store.update(k) { oldV =>
      Monoid.nonZeroOption(oldV.map { Monoid.plus(_,v) } getOrElse v)
    }
  }

  protected def multiIncrement(store: StoreType, items: Map[(Key,BatchID),Value]): Future[StoreType] =
    items.foldLeft(Future.value(store)) { (storeFuture, pair) =>
      storeFuture.flatMap { increment(_, pair) }
    }

  protected def incStore(item: ((Key,BatchID), Value), callback: Future[StoreType] => Future[StoreType]) {
    futureQueue.flatMapLast { s => callback(increment(s, item)) }
  }

  protected def multiIncStore(optItems: Option[Map[(Key,BatchID),Value]],
                              callback: Future[StoreType] => Future[StoreType]) {
    optItems match {
      case Some(items) => futureQueue.flatMapLast { s => callback(multiIncrement(s, items)) }
      case None => futureQueue.transformLast { callback(_) }
    }
  }

  override def execute(tuple: Tuple) {
    def handleSuccessAndFailure(storeFuture: Future[StoreType]): Future[StoreType] =
      storeFuture
        .onSuccess { _ =>
          successHandlerBox.get.handlerFn()
          collector.ack(tuple)
        }
        .rescue { exceptionHandlerBox.get.handlerFn.andThen { _ => storeFuture } }

    if(tuple.getSourceComponent == DRPC_DECODER)
      executeDRPC(tuple)
    else {
      val pair = unpack(tuple)
      cacheCount match {
        case Some(_) => multiIncStore(buffer(Map(pair)), handleSuccessAndFailure)
        case None => incStore(pair, handleSuccessAndFailure)
      }
    }
  }

  override def declareOutputFields(dec : OutputFieldsDeclarer) {
    dec.declareStream(RPC_STREAM, new Fields(VALUE_FIELD, RETURN_INFO))
  }

  override def cleanup { futureQueue.last.foreach { _.close } }
  override val getComponentConfiguration = null
}
