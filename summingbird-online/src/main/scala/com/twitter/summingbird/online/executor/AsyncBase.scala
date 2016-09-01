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

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.summingbird.online.FutureQueue
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, MaxFutureWaitTime, MaxWaitingFutures }
import com.twitter.util._
import org.slf4j.{ Logger, LoggerFactory }

import scala.util.{ Failure, Success, Try }

abstract class AsyncBase[I, O, S, D, RC](maxWaitingFutures: MaxWaitingFutures, maxWaitingTime: MaxFutureWaitTime, maxEmitPerExec: MaxEmitPerExecute) extends Serializable with OperationContainer[I, O, S, D, RC] {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * If you can use Future.value below, do so. The double Future is here to deal with
   * cases that need to complete operations after or before doing a FlatMapOperation or
   * doing a store merge
   */
  def apply(state: S, in: I): Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]]
  def tick: Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]] = Future.value(Nil)

  private[executor] lazy val futureQueue = new FutureQueue[Seq[S], TraversableOnce[O]](maxWaitingFutures, maxWaitingTime, maxEmitPerExec)

  override def executeTick =
    finishExecute(Nil, tick)

  override def execute(state: S, data: I) =
    finishExecute(List(state), apply(state, data))

  private def finishExecute(states: Seq[S], fIn: Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]]) = {
    futureQueue.addAllFuture(states, fIn)

    // always empty the responses
    futureQueue.dequeue
  }

  private def handleSuccess(fut: Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]]) =
    fut.onSuccess { iter => futureQueue.addAll(iter) }
}
