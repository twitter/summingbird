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

import com.twitter.algebird.{ Monoid, SummingQueue }
import com.twitter.chill.MeatLocker
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.batch.BatchID
import com.twitter.util.Future
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.online.FlatMapOperation
import java.util.Date
import _root_.akka.routing.ConsistentHashingRouter.ConsistentHashable
import _root_.akka.actor.Actor
import com.twitter.summingbird.online.executor.DataInjection
import com.twitter.summingbird.batch.{BatchID, Timestamp}


class SourceActor[Input, InputWireFmt <: ConsistentHashable]
    (akkaSrc: AkkaSource[Input], targetNames: List[String], encoder: DataInjection[Input, InputWireFmt]) extends Actor {
  import context._
  val targets = targetNames.map { actorName => context.actorSelection("../../" + actorName) }

  case object Emit
  override def preStart = {
    Thread.sleep(200)
    self ! Emit
  }

  def receive = {
    case Emit => {
      akkaSrc.isFinished match {
        case true => context.stop(self)
        case false =>
          akkaSrc.poll.map { d =>
            d.foreach { p => targets.foreach { t =>
              t ! Data(encoder(p)) } }
            self ! Emit
          }
      }
    }
  }
}



