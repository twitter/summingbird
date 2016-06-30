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

import java.util.concurrent.CyclicBarrier

import com.twitter.bijection.Injection
import com.twitter.conversions.time._
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, MaxFutureWaitTime, MaxWaitingFutures }
import com.twitter.util._
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Seconds, Span }

class AsyncBaseSpec extends WordSpec with Eventually {

  def verifyWaitN[T](futuresCount: Int, waitOn: Int, valueToFill: Try[T]) = {
    val ps = 0.until(futuresCount).map { _ => Promise[Unit]() }.toArray

    val t = new Thread {
      @volatile var unblocked = false
      override def run() = {
        Await.result(AsyncBase.waitN(ps, waitOn))
        unblocked = true
      }
    }
    t.start

    for (i <- 0 until Math.min(futuresCount, waitOn)) {
      assert(t.unblocked === false)
      valueToFill match {
        case Return(v) =>
          ps(i).setValue(v)
        case Throw(e) =>
          ps(i).setException(e)
      }
    }
    eventually(timeout(Span(5, Seconds)))(assert(t.unblocked === true))
    t.join
  }

  "waitN should wait for exactly n futures to finish " in {
    for {
      futuresCount <- 0 until 10
      waitOn <- 0 until 10
      valueToFill <- Seq(Return(()), Throw(new Exception))
    } verifyWaitN(futuresCount, waitOn, valueToFill)
  }

  "AsyncBase should force only the needed number of extra futures " in {
    val totalPromisesCount = 5
    val maxWaitingFutures = 2

    /**
     * What follows is a bit complex, here's the idea.
     * We create an AsyncBase that consumes from a sequence of unfulfilled promises.
     * We put synchronization points before and after calling execute method of
     * AsyncBase. This way we control execution.
     * We let AsyncBase move forward in a controlled way using the synchronization
     * barriers and assert at appropriate places. We control the number of outstanding
     * futures by selectively fulfilling the promises.
     */
    val promises = 0.until(totalPromisesCount).map { _ => Promise[Traversable[Int]]() }.toArray

    val ab = new AsyncBase[Int, Int, Unit, Int, Unit](
      MaxWaitingFutures(maxWaitingFutures),
      MaxFutureWaitTime(1.minute),
      MaxEmitPerExecute(Int.MaxValue)
    ) {
      var promiseDelivered = 0
      override def apply(state: Unit, in: Int): Future[TraversableOnce[(Seq[Unit], Future[TraversableOnce[Int]])]] = {
        // Return one promise at a time
        val ret = Future.value(Seq((Seq(()), promises(promiseDelivered))))
        promiseDelivered += 1
        ret
      }

      override def decoder: Injection[Int, Int] = implicitly
      override def encoder: Injection[Int, Int] = implicitly
    }

    val beforeExecute = new CyclicBarrier(2)
    val afterExecute = new CyclicBarrier(2)

    val t = new Thread {
      override def run(): Unit = {
        for (i <- 0 until totalPromisesCount) {
          beforeExecute.await()
          ab.execute((), 1)
          afterExecute.await()
        }
      }
    }

    t.start

    // Let two promises get queued up
    beforeExecute.await()
    afterExecute.await()
    beforeExecute.await()
    afterExecute.await()

    // Two promises have been processed, they should get queued up
    assert(ab.outstandingFuturesQueue.size == 2)

    beforeExecute.await()
    Thread.sleep(1000)
    // t should block now, unblock it by clearing two
    promises(0).setValue(Seq(0))
    promises(2).setValue(Seq(0))
    afterExecute.await()
    assert(ab.outstandingFuturesQueue.size == ab.maxWaitingFuturesLowerWaterMark)

    beforeExecute.await()
    afterExecute.await()
    beforeExecute.await()
    Thread.sleep(1000)
    // t should block now, unblock it by clearing two
    promises(1).setValue(Seq(0))
    promises(3).setValue(Seq(0))
    afterExecute.await()

    // t should get unblocked now and stay unblocked
    assert(ab.outstandingFuturesQueue.size == ab.maxWaitingFuturesLowerWaterMark)
    ab.outstandingFuturesQueue.foreach { f => assert(!f.isDefined) }
  }
}
