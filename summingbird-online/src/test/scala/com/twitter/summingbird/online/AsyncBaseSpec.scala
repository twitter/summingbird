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

package com.twitter.summingbird.online

import com.twitter.summingbird.online.executor.AsyncBase
import com.twitter.util._
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Span }

class AsyncBaseSpec extends WordSpec with Eventually {

  def onThread(f: => Unit): Thread = {
    new Thread {
      override def run() = {
        f
      }
    }
  }

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
    eventually(timeout(Span(500, Millis)))(assert(t.unblocked === true))
    t.join
  }

  "waitN should wait for exactly n futures to finish " in {
    for {
      futuresCount <- 0 until 10
      waitOn <- 0 until 10
      valueToFill <- Seq(Return(()), Throw(new Exception))
    } verifyWaitN(futuresCount, waitOn, valueToFill)
  }
}
