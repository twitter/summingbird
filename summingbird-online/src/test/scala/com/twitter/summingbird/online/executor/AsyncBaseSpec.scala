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
import com.twitter.util.{ Future, Promise }
import org.scalatest.WordSpec
import scala.util.Try

class AsyncBaseSpec extends WordSpec {
  val tickData = Future(Seq((Seq(100, 104, 99), Future(Seq(9, 10, 13))), (Seq(12, 19), Future(Seq(100, 200, 500)))))
  val dequeueData = List((Seq(8, 9), Try(Seq(4, 5, 6))))

  class TestFutureQueue extends FutureQueue[Seq[Int], TraversableOnce[Int]](
    MaxWaitingFutures(100),
    MaxFutureWaitTime(1.minute),
    MaxEmitPerExecute(Int.MaxValue)
  ) {
    var added = false
    var dequeued = false
    var addedAll: TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])] = _
    var addedAllFuture: Future[TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]] = _
    var addedAllFutureState: Seq[Int] = _

    override def add(state: Seq[Int], fut: Future[TraversableOnce[Int]]) =
      throw new RuntimeException("not implemented")

    override def addAll(
      iter: TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]): Int = synchronized {
      assert(!added)
      added = true
      addedAll = iter
      0
    }

    override def addAllFuture(
      state: Seq[Int],
      iterFut: Future[TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]]): Unit = synchronized {
      assert(!added)
      added = true
      addedAllFutureState = state
      addedAllFuture = iterFut
    }

    override def dequeue: TraversableOnce[(Seq[Int], Try[TraversableOnce[Int]])] = synchronized {
      assert(!dequeued)
      dequeued = true
      dequeueData
    }
  }

  "Queues tick with Nil on executeTick" in {
    val queue = new TestFutureQueue

    val ab = new AsyncBase[Int, Int, Int, Int, Unit](
      MaxWaitingFutures(100),
      MaxFutureWaitTime(1.minute),
      MaxEmitPerExecute(Int.MaxValue)
    ) {
      override lazy val futureQueue = queue
      override def apply(state: Int, in: Int) = throw new RuntimeException("not implemented")
      override def tick: Future[TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]] = tickData
      override def decoder: Injection[Int, Int] = implicitly
      override def encoder: Injection[Int, Int] = implicitly
    }

    assert(ab.executeTick === dequeueData)
    assert(queue.added)
    assert(queue.addedAllFuture === tickData)
    assert(queue.addedAllFutureState === Nil)
  }

  "Queues with state on apply" in {
    val queue = new TestFutureQueue
    val state = 1089

    val ab = new AsyncBase[Int, Int, Int, Int, Unit](
      MaxWaitingFutures(100),
      MaxFutureWaitTime(1.minute),
      MaxEmitPerExecute(Int.MaxValue)
    ) {
      override lazy val futureQueue = queue
      override def apply(state: Int, in: Int) = tickData
      override def tick: Future[TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]] = throw new RuntimeException("not implemented")
      override def decoder: Injection[Int, Int] = implicitly
      override def encoder: Injection[Int, Int] = implicitly
    }

    assert(ab.execute(state, 5) === dequeueData)
    assert(queue.added)
    assert(queue.addedAllFuture === tickData)
    assert(queue.addedAllFutureState === List(state))
  }
}
