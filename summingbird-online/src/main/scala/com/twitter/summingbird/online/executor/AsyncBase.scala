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

import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.online.TrimmableQueue
import com.twitter.summingbird.online.option.{MaxWaitingFutures, MaxFutureWaitTime}
import scala.collection.mutable.SynchronizedQueue
import com.twitter.util.{Await, Duration, Future}
import scala.util.{Try, Success, Failure}
import java.util.concurrent.TimeoutException
import org.slf4j.{LoggerFactory, Logger}


abstract class AsyncBase[I,O,InputWireFmnt,OutputWireFmnt]
            (maxWaitingFutures: MaxWaitingFutures, maxWaitingTime: MaxFutureWaitTime)
               extends Serializable with OperationContainer[I, O, InputWireFmnt, OutputWireFmnt] {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /** If you can use Future.value below, do so. The double Future is here to deal with
   * cases that need to complete operations after or before doing a FlatMapOperation or
   * doing a store merge
   */
  def apply(tup: InputState[InputWireFmnt], in: (Timestamp, I)): Future[Iterable[(List[InputState[InputWireFmnt]], Future[TraversableOnce[(Timestamp, O)]])]]
  def tick: Future[Iterable[(List[InputState[InputWireFmnt]], Future[TraversableOnce[(Timestamp, O)]])]] = Future.value(Nil)

  private lazy val outstandingFutures = new SynchronizedQueue[Future[Unit]] with TrimmableQueue[Future[Unit]]
  private lazy val responses = new SynchronizedQueue[(List[InputState[InputWireFmnt]], Try[TraversableOnce[(Timestamp, O)]])]()

  // No data means its just a tick packet
  override def execute(inputState: Option[InputState[InputWireFmnt]], data: Option[(Timestamp, I)]) = {
    val fut = (inputState, data) match {
      case (Some(state), Some(tsIn)) => apply(state, tsIn)
      case _ => tick
    }

    fut.onSuccess { iter: Iterable[(List[InputState[InputWireFmnt]], Future[TraversableOnce[(Timestamp, O)]])] =>

        // Collect the result onto our responses
        val iterSize = iter.foldLeft(0) { case (iterSize, (tups, res)) =>
          res.onSuccess { t => responses += ((tups, Success(t))) }
          res.onFailure { t => responses += ((tups, Failure(t))) }
          // Make sure there are not too many outstanding:
          if(!res.isDefined) {
            outstandingFutures += res.unit
            iterSize + 1
          } else {
            iterSize
          }
        }

        if(outstandingFutures.size > maxWaitingFutures.get) {
          /*
           * This can happen on large key expansion.
           * May indicate maxWaitingFutures is too low.
           */
          logger.debug(
            "Exceeded maxWaitingFutures({}), put {} futures", maxWaitingFutures.get, iterSize
          )
        }
      }
      .onFailure { thr =>
        throw thr
       responses += ((inputState.map(List(_)).getOrElse(Nil), Failure(thr))) }

    if(!fut.isDefined) {
      outstandingFutures += fut.unit
    }

    // always empty the responses
    emptyQueue
  }

  private def forceExtraFutures {
    outstandingFutures.dequeueAll(_.isDefined)
    val toForce = outstandingFutures.trimTo(maxWaitingFutures.get)
    if(!toForce.isEmpty) {
      try {
        Await.ready(Future.collect(toForce), maxWaitingTime.get)
      }
      catch {
        case te: TimeoutException =>
          logger.error("forceExtra failed on %d Futures".format(toForce.size), te)
      }
    }
  }

  private def emptyQueue = {
    // don't let too many futures build up
    forceExtraFutures
    // Take all results that have been placed for writing to the network
    responses.dequeueAll(_ => true).toList
  }
}
