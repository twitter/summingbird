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

import com.twitter.util.{ Await, Future, Promise }
import com.twitter.algebird.util.summer.AsyncSummer
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.algebra.Mergeable

import com.twitter.summingbird.online.{ FlatMapOperation, Externalizer }
import com.twitter.summingbird.online.option._

import chain.Chain

// These CMaps we generate in the FFM, we use it as an immutable wrapper around
// a mutable map.
import scala.util.control.NonFatal

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
 * @author Ian O Connell
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class Summer[Key, Value: Semigroup, Event, S](
  @transient storeSupplier: () => Mergeable[Key, Value],
  @transient flatMapOp: FlatMapOperation[(Key, (Option[Value], Value)), Event],
  @transient successHandler: OnlineSuccessHandler,
  @transient exceptionHandler: OnlineExceptionHandler,
  summerBuilder: SummerBuilder,
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  maxEmitPerExec: MaxEmitPerExecute,
  includeSuccessHandler: IncludeSuccessHandler
) extends AsyncBase[Iterable[(Key, Value)], Event, InputState[S]](
  maxWaitingFutures,
  maxWaitingTime,
  maxEmitPerExec
) {
  val lockedOp = Externalizer(flatMapOp)

  val storeBox = Externalizer(storeSupplier)
  lazy val storePromise = Promise[Mergeable[Key, Value]]
  lazy val store = Await.result(storePromise)

  lazy val sSummer: AsyncSummer[(Key, (Chain[InputState[S]], Value)), Map[Key, (Chain[InputState[S]], Value)]] =
    summerBuilder.getSummer[Key, (Chain[InputState[S]], Value)](implicitly[Semigroup[(Chain[InputState[S]], Value)]])

  val exceptionHandlerBox = Externalizer(exceptionHandler.handlerFn.lift)
  val successHandlerBox = Externalizer(successHandler)
  var successHandlerOpt: Option[OnlineSuccessHandler] = null

  override def init(): Unit = {
    super.init()
    storePromise.setValue(storeBox.get())
    store.toString // Do the lazy evaluation now so we can connect before tuples arrive.

    successHandlerOpt = if (includeSuccessHandler.get) Some(successHandlerBox.get) else None
  }

  override def notifyFailure(inputs: Chain[InputState[S]], error: Throwable): Unit = {
    super.notifyFailure(inputs, error)
    exceptionHandlerBox.get.apply(error)
  }

  private def handleResult(kvs: Map[Key, (Chain[InputState[S]], Value)]): TraversableOnce[(Chain[InputState[S]], Future[TraversableOnce[Event]])] =
    store.multiMerge(kvs.mapValues(_._2)).iterator.map {
      case (k, beforeF) =>
        val (tups, delta) = kvs(k)
        (tups, beforeF.flatMap { before =>
          lockedOp.get.apply((k, (before, delta)))
        }.onSuccess { _ => successHandlerOpt.get.handlerFn.apply(()) })
    }.toList

  override def tick = sSummer.tick.map(handleResult(_))

  override def apply(state: InputState[S], innerTuples: Iterable[(Key, Value)]) = {
    try {
      assert(innerTuples.nonEmpty, "Maps coming in must not be empty")
      state.fanOut(innerTuples.size)
      val cacheEntries = innerTuples.map {
        case (k, v) =>
          (k, (Chain.single(state), v))
      }

      sSummer.addAll(cacheEntries).map(handleResult(_))
    } catch {
      case NonFatal(e) => Future.exception(e)
    }
  }

  override def cleanup(): Unit = Await.result(store.close)
}
