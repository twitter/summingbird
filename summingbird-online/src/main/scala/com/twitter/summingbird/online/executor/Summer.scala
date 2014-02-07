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

package com.twitter.summingbird.online.executor

import com.twitter.util.{Await, Future}
import com.twitter.algebird.{Semigroup, SummingQueue}
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.bijection.Injection

import com.twitter.summingbird.online.{Externalizer, AsyncCache}
import com.twitter.summingbird.batch.{BatchID, Timestamp}
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option.CacheSize



/**
  * The SummerBolt takes two related options: CacheSize and MaxWaitingFutures.
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

class Summer[Key, Value: Semigroup, S, D](
  @transient storeSupplier: () => MergeableStore[(Key,BatchID), Value],
  @transient successHandler: OnlineSuccessHandler,
  @transient exceptionHandler: OnlineExceptionHandler,
  cacheBuilder: (Semigroup[(List[S], Timestamp, Value)]) => AsyncCache[(Key, BatchID), (List[S], Timestamp, Value)],
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  includeSuccessHandler: IncludeSuccessHandler,
  pDecoder: Injection[(Timestamp, ((Key, BatchID), Value)), D],
  pEncoder: Injection[(Timestamp, (Key, (Option[Value], Value))), D]) extends
    AsyncBase[((Key, BatchID), Value), (Key, (Option[Value], Value)), S, D](
      maxWaitingFutures,
      maxWaitingTime) {

  val encoder = pEncoder
  val decoder = pDecoder

  val storeBox = Externalizer(storeSupplier)
  lazy val store = storeBox.get.apply

  // See MaxWaitingFutures for a todo around removing this.
  lazy val sCache: AsyncCache[(Key, BatchID), (List[S], Timestamp, Value)] = cacheBuilder(implicitly[Semigroup[(List[S], Timestamp, Value)]])

  val exceptionHandlerBox = Externalizer(exceptionHandler.handlerFn.lift)
  val successHandlerBox = Externalizer(successHandler)

  var successHandlerOpt: Option[OnlineSuccessHandler] = null

  override def init {
    super.init
    successHandlerOpt = if (includeSuccessHandler.get) Some(successHandlerBox.get) else None
  }

  override def notifyFailure(inputs: List[S], error: Throwable): Unit = {
    super.notifyFailure(inputs, error)
    exceptionHandlerBox.get.apply(error)
  }

  private def handleResult(kvs: Map[(Key, BatchID), (List[S], Timestamp, Value)])
                        : Iterable[(List[S], Future[TraversableOnce[(Timestamp, (Key, (Option[Value], Value)))]])] = {
    store.multiMerge(kvs.mapValues(_._3)).map{ case (innerKb, beforeF) =>
      val (tups, stamp, delta) = kvs(innerKb)
      val (k, _) = innerKb
      (tups, beforeF.map(before => List((stamp, (k, (before, delta)))))
        .onSuccess { _ => successHandlerOpt.get.handlerFn.apply() } )
    }
    .toList // force, but order does not matter, so we could optimize this
  }

  override def tick = sCache.tick.map(handleResult(_))

  override def apply(state: S,
                     tsIn: (Timestamp, ((Key, BatchID), Value))) = {
    val (ts, (kb, v)) = tsIn
    sCache.insert(List(kb -> (List(state), ts, v))).map(handleResult(_))
  }

  override def cleanup { Await.result(store.close) }
}
