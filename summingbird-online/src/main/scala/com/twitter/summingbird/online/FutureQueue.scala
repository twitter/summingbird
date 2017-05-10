/*
Copyright 2016 Twitter, Inc.

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

package com.twitter.summingbird.online

import com.twitter.bijection.twitter_util.UtilBijections
import com.twitter.summingbird.online.option.{ MaxFutureWaitTime, MaxWaitingFutures }
import com.twitter.util.{ Await, Future, Promise }
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.{ Logger, LoggerFactory }
import scala.util.Try

private[summingbird] object FutureQueue {
  /**
   * Ratio of total number of outstanding futures to the portion that is finished
   * ,at which finished futures are cleared.
   * Clearing finished futures costs proportional to total number of outstanding
   * futures, so we want to make sure we only clear when sufficient portion is
   * finished.
   */
  val OutstandingFuturesDequeueRatio = 2

  /**
   * Wait for n futures to finish. Doesn't block, the returned future is satisfied
   * once n futures have finished either successfully or unsuccessfully.
   * If n is greater than number of futures in queue then we wait on all of them.
   */
  def waitN[A](fs: Iterable[Future[A]], n: Int): Future[Unit] = {
    val waitOnCount = Math.min(fs.size, n)
    if (waitOnCount <= 0) {
      Future.Unit
    } else {
      val count = new AtomicInteger(waitOnCount)
      val p = Promise[Unit]()
      fs.foreach { f =>
        f.ensure {
          // Note that since we are only decrementing we can cross 0 only
          // once (unless we decrement more than 2^32 times).
          if (count.decrementAndGet() == 0) {
            p.setValue(())
          }
        }
      }
      p
    }
  }
}

/**
 * Use this class to do lazy continuations.  This is especially useful
 * when using a single thread that initiates Futures and must also handle
 * the continuation.
 *
 * type S: state associated with each Future
 * type T: the result type of each Future
 *
 * Minimally, this queue accepts a state value along with a Future.  Once
 * the Future completes, the result can be retrieved, along with the state,
 * via the dequeue method.
 *
 * To support batching, inputs can also be TraversableOnce[(S, Future[T])]
 */
private[summingbird] class FutureQueue[S, T](
    maxWaitingFutures: MaxWaitingFutures,
    maxWaitingTime: MaxFutureWaitTime) {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Track the futures that may be outstanding.  We may need to wait on some
  // of them in case the actual outstanding count gets too high.
  private lazy val outstandingFutures = Queue.linkedNonBlocking[Future[Unit]]
  // Track the number of actually outstanding futures
  private[online] lazy val numPendingOutstandingFutures = new AtomicInteger(0)
  // When futures complete, they deposit their results here.
  private lazy val responses = Queue.linkedNonBlocking[(S, Try[T])]

  private val tryBijection = UtilBijections.twitter2ScalaTry[T]

  /**
   * Add a Future with state onto the queue.  This can eventually be
   * dequeued as (S, Try[T]) when the Future completes.
   */
  def add(state: S, fut: Future[T]): Unit = addInternal(state, fut)

  private def addInternal(state: S, fut: Future[T]): Boolean = {
    val responded =
      fut.respond { t => responses.put((state, tryBijection(t))) }
    addOutstandingFuture(responded.unit)
  }

  /**
   * Add a collection onto the queue.  Each associated pair can
   * eventually be dequeued as (S, Try[T]) when the associated
   * Future completes.
   */
  def addAll(iter: TraversableOnce[(S, Future[T])]): Unit = {
    val addedSize = iter.count {
      case (state, fut) => addInternal(state, fut)
    }

    if (outstandingFutures.size > maxWaitingFutures.get) {
      /*
       * This can happen on large key expansion.
       * May indicate maxWaitingFutures is too low.
       */
      logger.debug(
        "Exceeded maxWaitingFutures({}), put {} futures", maxWaitingFutures.get, addedSize
      )
    }
  }

  private def addOutstandingFuture(fut: Future[Unit]): Boolean =
    if (!fut.isDefined) {
      numPendingOutstandingFutures.incrementAndGet
      val ensured = fut.ensure(numPendingOutstandingFutures.decrementAndGet)
      outstandingFutures.put(ensured)
      true
    } else {
      false
    }

  private def forceExtraFutures(): Unit = {
    val maxWaitingFuturesCount = maxWaitingFutures.get
    val pendingFuturesCount = numPendingOutstandingFutures.get
    if (pendingFuturesCount > maxWaitingFuturesCount) {
      // Too many futures waiting, let's clear.
      val pending = outstandingFutures.toSeq.filterNot(_.isDefined)
      val toClear = pending.size - maxWaitingFuturesCount
      if (toClear > 0) {
        try {
          Await.ready(FutureQueue.waitN(pending, toClear), maxWaitingTime.get)
        } catch {
          case te: TimeoutException =>
            logger.error(s"forceExtra failed on $toClear Futures", te)
        }
        outstandingFutures.putAll(pending.filterNot(_.isDefined))
      } else {
        outstandingFutures.putAll(pending)
      }
    } else {
      // only dequeueAll if there's bang for the buck
      if (outstandingFutures.size >= FutureQueue.OutstandingFuturesDequeueRatio * pendingFuturesCount) {
        outstandingFutures.dequeueAll(_.isDefined)
      }
    }
  }

  /**
   * Retrieve any completed results, along with their associated states.
   *
   * Returns up to maxItems items.
   */
  def dequeue(maxItems: Int): Seq[(S, Try[T])] = {
    // don't let too many futures build up
    forceExtraFutures()
    // Take all results that have been placed for writing to the network
    responses.take(maxItems)
  }
}
