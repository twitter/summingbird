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
import com.twitter.conversions.time._
import com.twitter.summingbird.online.option.{ MaxFutureWaitTime, MaxWaitingFutures }
import com.twitter.util._
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Seconds, Span }
import scala.util.{ Failure, Random, Success }

case class NonNegativeShort(get: Short) {
  require(get >= 0)
}

class FutureQueueLaws extends Properties("FutureQueue") with Eventually {
  def genTry[T](implicit arb: Arbitrary[T]): Gen[Try[T]] = Gen.oneOf(arb.arbitrary.map(Return(_)), Arbitrary.arbitrary[java.lang.Exception].map(Throw(_)))
  implicit def arbTry[T: Arbitrary] = Arbitrary(genTry[T])

  implicit val arbNonNegativeShort: Arbitrary[NonNegativeShort] = Arbitrary(
    Arbitrary.arbitrary[Short].filter { _ >= 0 }.map { NonNegativeShort }
  )

  val twitterToScala = UtilBijections.twitter2ScalaTry[String]

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
        MaxFutureWaitTime(20.seconds)
      )
      fq.addAll((0 until futuresCount.get).map { _ =>
        () -> Promise[Unit]
      })
      val start = Time.now
      val res = fq.dequeue(futuresCount.get)
      val end = Time.now
      res.isEmpty &&
        (end - start < 15.seconds)
      fq.numPendingOutstandingFutures.get == futuresCount.get
    }

  property("preserves status of Future.const via addAll") =
    forAll { inputs: Seq[(String, Try[String])] =>
      val count = inputs.size
      val fq = new FutureQueue[String, String](
        MaxWaitingFutures(count + 1),
        MaxFutureWaitTime(20.seconds)
      )
      fq.addAll(inputs.map {
        case (state, t) =>
          state -> Future.const(t)
      })
      fq.dequeue(count) == inputs.map {
        case (state, t) =>
          state -> twitterToScala(t)
      }
    }

  property("preserves status of Future.const via add") =
    forAll { inputs: Seq[(String, Try[String])] =>
      val count = inputs.size
      val fq = new FutureQueue[String, String](
        MaxWaitingFutures(count + 1),
        MaxFutureWaitTime(20.seconds)
      )
      inputs.foreach {
        case (state, t) =>
          fq.add(state, Future.const(t))
      }
      fq.dequeue(count) == inputs.map {
        case (state, t) =>
          state -> twitterToScala(t)
      }
    }

  property("accounts for completed futures") =
    forAll { (incomplete: NonNegativeShort, complete: NonNegativeShort) =>
      val incompleteFuture = Promise[Unit]
      val completeFuture = Promise[Unit]
      val incompleteFutures = Seq.fill(incomplete.get)(incompleteFuture)
      val completeFutures = Seq.fill(complete.get)(completeFuture)
      val mixedFutures = Random.shuffle(incompleteFutures ++ completeFutures)

      val fq = new FutureQueue[Unit, Unit](
        MaxWaitingFutures(1),
        MaxFutureWaitTime(20.seconds)
      )
      fq.addAll(mixedFutures.map { () -> _ })

      val initialPendingCount = fq.numPendingOutstandingFutures.get
      completeFuture.setValue(())

      initialPendingCount == (incomplete.get + complete.get) &&
        fq.numPendingOutstandingFutures.get == incomplete.get
    }
}
