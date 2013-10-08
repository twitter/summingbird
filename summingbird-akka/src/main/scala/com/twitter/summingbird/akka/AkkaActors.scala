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
import com.twitter.summingbird.akka.option._
import com.twitter.util.Future
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import java.util.{ Date, Map => JMap }

import java.util.{ Map => JMap }

import _root_.akka.routing.ConsistentHashingRouter.ConsistentHashable
import _root_.akka.actor.Actor

case class FlatMapOutput(data: Any) extends ConsistentHashable {
  override def consistentHashKey: Any = {
    try {
      data.asInstanceOf[Tuple2[Any, Any]]._1
    } catch {
      case _: Throwable => data
    }
  }
}

class SourceActor(akkaSrc: AkkaSource[_], targetNames: List[String]) extends Actor {
  import context._
  val targets = targetNames.map { actorName => context.actorSelection("../../" + actorName) }

  case object Emit()
  override def preStart = {
    Thread.sleep(200)
    self ! Emit()
  }

  def receive = {
    case s: Emit => {
      akkaSrc.isFinished match {
        case true => context.stop(self)
        case false =>
          akkaSrc.poll.map { d =>
            d.foreach { p => targets.foreach { t => t ! FlatMapOutput(p) } }
            self ! Emit()
          }
      }
    }
  }
}

class FlatMapActor(op: FlatMapOperation[Any, Any], targetNames: List[String]) extends Actor {
  import context._
  val targets = targetNames.map { actorName => context.actorSelection("../../" + actorName) }

  private def send(operated: Any) = {
    val traversable = operated.asInstanceOf[TraversableOnce[Any]]
    traversable.foreach {data => 
    	targets.foreach { t => t ! FlatMapOutput(data) }
    }
  }
  def receive = {
    case FlatMapOutput(d) =>
      val operated = op(d)
      if (operated.isInstanceOf[com.twitter.util.Future[Any]])
        operated.asInstanceOf[com.twitter.util.Future[Any]].map {r => send(r) }
      else
        send(operated)
  }
}

class SummerActor[Key, Value: Monoid](
  storeBuilder: () => MergeableStore[(Key, BatchID), Value])(implicit batcher: Batcher) extends Actor {
  import context._
  import Constants._
  val storeBox = MeatLocker(storeBuilder)
  lazy val store = storeBox.get.apply
  lazy val cacheCount = Some(0)
  lazy val buffer = SummingQueue[Map[(Key, BatchID), Value]](cacheCount.getOrElse(0))
  lazy val futureQueue = FutureQueue(Future.Unit, 10)
  // TODO (https://github.com/twitter/tormenta/issues/1): Think about
  // how this can help with Tormenta's open issue for a tuple
  // conversion library. Storm emits Values and receives Tuples.
  def unpack(x: (Any, Any)) = {
    val batchID = batcher.batchOf(new Date(0))
    val key = x._1.asInstanceOf[Key]
    val value = x._2.asInstanceOf[Value]
    ((key, batchID), value)
  }

  def receive = {
    case FlatMapOutput(input) =>
      buffer(Map(unpack(input.asInstanceOf[(Any, Any)]))).foreach { pairs =>
      val futures = pairs.map(store.merge(_)).toList
      futureQueue += Future.collect(futures).unit
    }
  }

  override def postStop() { store.close }
}


 

