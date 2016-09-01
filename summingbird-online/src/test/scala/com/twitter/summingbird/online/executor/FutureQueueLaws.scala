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

package com.twitter.summingbird.online.executor

import com.twitter.conversions.time._
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, MaxFutureWaitTime, MaxWaitingFutures }
import com.twitter.util._
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Seconds, Span }
import scala.util.{ Failure, Success }

case class NonNegativeShort(get: Short) {
  require(get >= 0)
}

class FutureQueueLaws extends Properties("FutureQueue") with Eventually {
  def genTry[T](implicit arb: Arbitrary[T]): Gen[Try[T]] = Gen.oneOf(arb.arbitrary.map(Return(_)), Arbitrary.arbitrary[java.lang.Exception].map(Throw(_)))
  implicit def arbTry[T: Arbitrary] = Arbitrary(genTry[T])

  implicit val arbNonNegativeShort: Arbitrary[NonNegativeShort] = Arbitrary(
    Arbitrary.arbitrary[Short].filter { _ >= 0 }.map { NonNegativeShort }
  )
  def twitterToScala[T](t: Try[T]) = t match {
    case Return(v) => Success(v)
    case Throw(t) => Failure(t)
  }

  property("waitN should wait for exactly n futures to finish") =
    forAll { (futuresCount: NonNegativeShort, waitOn: NonNegativeShort, valueToFill: Try[Unit]) =>
      val ps = 0.until(futuresCount.get).map { _ => Promise[Unit]() }.toArray

      val t = new Thread {
        @volatile var unblocked = false
        override def run() = {
          Await.result(FutureQueue.waitN(ps, waitOn.get))
          unblocked = true
        }
      }
      t.start

      for (i <- 0 until Math.min(futuresCount.get, waitOn.get)) {
        assert(t.unblocked == false)
        valueToFill match {
          case Return(v) =>
            ps(i).setValue(v)
          case Throw(e) =>
            ps(i).setException(e)
        }
      }
      eventually(timeout(Span(5, Seconds)))(assert(t.unblocked == true))
      t.join
      true
    }

  property("not block in dequeue if within bound") =
    forAll { (futuresCount: NonNegativeShort, slackSpace: NonNegativeShort) =>
      val fq = new FutureQueue[Unit, Unit](
        MaxWaitingFutures(futuresCount.get + slackSpace.get),
        MaxFutureWaitTime(20.seconds),
        MaxEmitPerExecute(futuresCount.get)
      )
      fq.addAll((0 until futuresCount.get).map { _ =>
        () -> Promise[Unit]
      })
      val start = Time.now
      val res = fq.dequeue
      val end = Time.now
      res.isEmpty &&
        (end - start < 15.seconds) &&
        fq.outstandingFutures.size == futuresCount.get
    }

  property("queue the initial future") =
    forAll { (futuresCount: NonNegativeShort) =>
      val fq = new FutureQueue[Unit, Unit](
        MaxWaitingFutures(futuresCount.get + 1),
        MaxFutureWaitTime(20.seconds),
        MaxEmitPerExecute(futuresCount.get)
      )
      val p = Promise[TraversableOnce[(Unit, Future[Unit])]]
      fq.addAllFuture((), p)
      fq.outstandingFutures.size == 1
    }

  property("queue the inner future") =
    forAll { (futuresCount: NonNegativeShort) =>
      val fq = new FutureQueue[Unit, Unit](
        MaxWaitingFutures(futuresCount.get + 1),
        MaxFutureWaitTime(20.seconds),
        MaxEmitPerExecute(futuresCount.get)
      )
      val p = Promise[TraversableOnce[(Unit, Future[Unit])]]
      fq.addAllFuture((), p)
      p.setValue((0 until futuresCount.get).map { _ =>
        () -> Promise[Unit]
      })

      fq.outstandingFutures.size == futuresCount.get + 1
    }

  property("addAllFuture yields the state and exception on failure") =
    forAll { (state: String, ex: Throwable) =>
      val fq = new FutureQueue[String, Unit](
        MaxWaitingFutures(10),
        MaxFutureWaitTime(20.seconds),
        MaxEmitPerExecute(10)
      )
      fq.addAllFuture(state, Future.exception(ex))
      fq.dequeue == Iterable((state, Failure(ex)))
    }

  property("preserves status of Future.const") =
    forAll { inputs: Seq[(String, Try[String])] =>
      val count = inputs.size
      val fq = new FutureQueue[String, String](
        MaxWaitingFutures(count + 1),
        MaxFutureWaitTime(20.seconds),
        MaxEmitPerExecute(count)
      )
      fq.addAll(inputs.map {
        case (state, t) =>
          state -> Future.const(t)
      })
      fq.dequeue == inputs.map {
        case (state, t) =>
          state -> twitterToScala(t)
      }
    }
}
