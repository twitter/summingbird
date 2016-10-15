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

import com.twitter.conversions.time._
import com.twitter.summingbird.online.FutureQueue
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, MaxFutureWaitTime, MaxWaitingFutures }
import com.twitter.util.{ Await, Future, Promise }
import org.scalatest.WordSpec
import scala.util.Try

class AsyncBaseSpec extends WordSpec {
  val data = Seq((Seq(100, 104, 99), Future(Seq(9, 10, 13))), (Seq(12, 19), Future(Seq(100, 200, 500))))
  val dequeueData = List((Seq(8, 9), Try(Seq(4, 5, 6))))

  class TestFutureQueue extends FutureQueue[Seq[Int], TraversableOnce[Int]](
    MaxWaitingFutures(100),
    MaxFutureWaitTime(1.minute)
  ) {
    var added = false
    var addedData: (Seq[Int], Future[TraversableOnce[Int]]) = _
    var addedAllData: TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])] = _
    var dequeued = false
    var dequeuedCount: Int = 0

    override def add(state: Seq[Int], fut: Future[TraversableOnce[Int]]): Unit = synchronized {
      assert(!added)
      added = true
      addedData = (state, fut)
    }

    override def addAll(
      iter: TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]): Unit = synchronized {
      assert(!added)
      added = true
      addedAllData = iter
    }

    override def dequeue(maxItems: Int): Seq[(Seq[Int], Try[TraversableOnce[Int]])] = synchronized {
      assert(!dequeued)
      dequeued = true
      dequeuedCount = maxItems
      dequeueData
    }
  }

  class TestAsyncBase(
    queue: TestFutureQueue,
    tickData: => Future[TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]] = throw new RuntimeException("not implemented"),
    applyData: => Future[TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]] = throw new RuntimeException("not implemented")) extends AsyncBase[Int, Int, Int](
    MaxWaitingFutures(100),
    MaxFutureWaitTime(1.minute),
    MaxEmitPerExecute(57)
  ) {
    override lazy val futureQueue = queue
    override def apply(state: Int, in: Int) = applyData
    override def tick = tickData
  }

  def promise = Promise[TraversableOnce[(Seq[Int], Future[TraversableOnce[Int]])]]

  "Queues tick on executeTick" in {
    val queue = new TestFutureQueue
    val p = promise
    val ab = new TestAsyncBase(queue, tickData = p)

    assert(ab.executeTick === dequeueData)
    assert(!queue.added)

    p.setValue(data)
    assert(queue.added)
    assert(queue.addedAllData === data)
    assert(queue.dequeuedCount === 57)
  }

  "Queues data on execute" in {
    val queue = new TestFutureQueue
    val p = promise
    val ab = new TestAsyncBase(queue, applyData = p)

    assert(ab.execute(1089, 5) === dequeueData)
    assert(!queue.added)

    p.setValue(data)
    assert(queue.added)
    assert(queue.addedAllData === data)
    assert(queue.dequeuedCount === 57)
  }

  "Queues state when executeTick fails" in {
    val queue = new TestFutureQueue
    val p = promise
    val ex = new RuntimeException("test fail 1")
    val ab = new TestAsyncBase(queue, tickData = p)

    assert(ab.executeTick === dequeueData)
    assert(!queue.added)

    p.setException(ex)
    assert(queue.added)
    assert(queue.addedData._1 === Nil)
    assert(ex === intercept[RuntimeException] { Await.result(queue.addedData._2) })
  }

  "Queues state when execute fails" in {
    val queue = new TestFutureQueue
    val p = promise
    val ex = new RuntimeException("test fail 2")
    val ab = new TestAsyncBase(queue, applyData = p)

    assert(ab.execute(1089, 5) === dequeueData)
    assert(!queue.added)

    p.setException(ex)
    assert(queue.added)
    assert(queue.addedData._1 === List(1089))
    assert(ex === intercept[RuntimeException] { Await.result(queue.addedData._2) })
  }
}
