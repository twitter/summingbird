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

package com.twitter.summingbird.akka

import scala.util.{Try, Success, Failure}

import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.online.option.MaxWaitingFutures
import com.twitter.summingbird.online.executor.OperationContainer
import com.twitter.summingbird.online.executor.InputState
import org.slf4j.{LoggerFactory, Logger}

import com.twitter.algebird.{ Monoid, SummingQueue }
import com.twitter.summingbird.batch.BatchID
import com.twitter.util.Future
import _root_.akka.actor.Actor
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
/**
 *
 * @author Ian O Connell
 */
case class BaseActor[I,O](
  targetNames: List[String],
  hasDependants: Boolean,
  executor: OperationContainer[I, O, WireType[I], WireType[O]]
  ) extends Actor {
  import context._
  val targets = targetNames.map { actorName => context.actorSelection("../../" + actorName) }

  system.scheduler.schedule(new FiniteDuration(1, TimeUnit.SECONDS), new FiniteDuration(200, TimeUnit.MILLISECONDS), self, TimedTick)

  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  private def fail(inputs: List[InputState[WireType[I]]], error: Throwable): Unit = {
    executor.notifyFailure(inputs, error)
    logger.error("Akka DAG of: {} tuples failed", inputs.size, error)
  }

  def processResults(curResults: TraversableOnce[(List[InputState[WireType[I]]], Try[TraversableOnce[(Timestamp, O)]])]) {
    curResults.foreach{ case (tups, res) =>
      res match {
        case Success(outs) => finish(tups, outs)
        case Failure(t) => fail(tups, t)
      }
    }
  }

 def receive = {
    case t@TimedTick =>
      processResults(executor.execute(None, None))
    case Data(raw) =>
      val w = raw.asInstanceOf[WireType[I]]
      val wrappedTuple = InputState(w)
      val tsIn = executor.decoder.inj.invert(w).get // Failing to decode here is an ERROR
      processResults(executor.execute(Some(wrappedTuple), Some(tsIn)))
    case t@_ => logger.error("Got unknown tuple: " + t)
  }


  private def finish(inputs: List[InputState[WireType[I]]], results: TraversableOnce[(Timestamp, O)]) {
    var emitCount = 0
    if(hasDependants) {
      results.foreach { result =>
        targets.foreach { t =>
          t ! Data(executor.encoder.inj(result)) }
        emitCount += 1
      }
    }

    logger.debug("actor finished processed {} linked tuples, emitted: {}", inputs.size, emitCount)
  }

  override def preStart {
    executor.init
  }

  override def postStop {
    executor.cleanup
  }
}
