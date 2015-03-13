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

import com.twitter.summingbird.online.Queue
import com.twitter.summingbird.online.option.{ MaxWaitingFutures, MaxFutureWaitTime, MaxEmitPerExecute }
import com.twitter.util.{ Await, Future }
import scala.util.{ Try, Success, Failure }
import java.util.concurrent.TimeoutException
import org.slf4j.{ LoggerFactory, Logger }

abstract class AsyncBase[I, O, S, D, RC](maxWaitingFutures: MaxWaitingFutures, maxWaitingTime: MaxFutureWaitTime, maxEmitPerExec: MaxEmitPerExecute) extends Serializable with OperationContainer[I, O, S, D, RC] {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * If you can use Future.value below, do so. The double Future is here to deal with
   * cases that need to complete operations after or before doing a FlatMapOperation or
   * doing a store merge
   */
  def apply(state: S, in: I): Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]]
  def tick: Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]] = Future.value(Nil)

  private lazy val outstandingFutures = Queue.linkedNonBlocking[Future[Unit]]
  private lazy val responses = Queue.linkedNonBlocking[(Seq[S], Try[TraversableOnce[O]])]

  override def executeTick =
    finishExecute(tick.onFailure { thr => responses.put(((Seq(), Failure(thr)))) })

  override def execute(state: S, data: I) =
    finishExecute(apply(state, data).onFailure { thr => responses.put(((List(state), Failure(thr)))) })

  private def finishExecute(fIn: Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]]) = {
    addOutstandingFuture(handleSuccess(fIn).unit)

    // always empty the responses
    emptyQueue
  }

  private def handleSuccess(fut: Future[TraversableOnce[(Seq[S], Future[TraversableOnce[O]])]]) =
    fut.onSuccess { iter: TraversableOnce[(Seq[S], Future[TraversableOnce[O]])] =>

      // Collect the result onto our responses
      val iterSize = iter.foldLeft(0) {
        case (iterSize, (tups, res)) =>
          res.onSuccess { t => responses.put(((tups, Success(t)))) }
          res.onFailure { t => responses.put(((tups, Failure(t)))) }
          // Make sure there are not too many outstanding:
          if (addOutstandingFuture(res.unit)) {
            iterSize + 1
          } else {
            iterSize
          }
      }
      if (outstandingFutures.size > maxWaitingFutures.get) {
        /*
           * This can happen on large key expansion.
           * May indicate maxWaitingFutures is too low.
           */
        logger.debug(
          "Exceeded maxWaitingFutures({}), put {} futures", maxWaitingFutures.get, iterSize
        )
      }
    }

  private def addOutstandingFuture(fut: Future[Unit]): Boolean =
    if (!fut.isDefined) {
      outstandingFutures.put(fut)
      true
    } else {
      false
    }

  private def forceExtraFutures() {
    outstandingFutures.dequeueAll(_.isDefined)
    val toForce = outstandingFutures.trimTo(maxWaitingFutures.get).toIndexedSeq
    if (toForce.nonEmpty) {
      try {
        Await.ready(Future.collect(toForce), maxWaitingTime.get)
      } catch {
        case te: TimeoutException =>
          logger.error("forceExtra failed on %d Futures".format(toForce.size), te)
      }
    }
  }

  private def emptyQueue = {
    // don't let too many futures build up
    forceExtraFutures()
    // Take all results that have been placed for writing to the network
    responses.take(maxEmitPerExec.get)
  }
}
