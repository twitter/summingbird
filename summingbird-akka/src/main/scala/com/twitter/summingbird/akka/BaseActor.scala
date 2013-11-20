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

import java.util.{ Map => JMap, Arrays => JArrays, List => JList }
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.akka.option.MaxWaitingFutures

import scala.collection.JavaConverters._

import org.slf4j.{LoggerFactory, Logger}
import _root_.akka.actor.Actor

/**
 *
 * @author Oscar Boykin
 * @author Ian O Connell
 * @author Sam Ritchie
 * @author Ashu Singhal
 */
abstract class BaseActor[I,O](
  hasDependants: Boolean,
  targetNames: List[String]
  ) extends Actor {
  import context._
  val targets = targetNames.map { actorName => context.actorSelection("../../" + actorName) }
  def decoder: AkkaTupleInjection[I]
  def encoder: AkkaTupleInjection[O]

  @transient protected lazy val logger: Logger =
    LoggerFactory.getLogger(getClass)

  protected def fail(inputs: JList[WireType[I]], error: Throwable): Unit = {
    logger.error("Akka DAG of: %d tuples failed".format(inputs.size), error)
  }

  private def send(payload: WireType[O]) =
     targets.foreach { t => t ! payload }

  protected def finish(inputs: JList[WireType[I]], results: TraversableOnce[(Timestamp, O)]) {
    var emitCount = 0
    if(hasDependants) {
      results.foreach { result =>
        send(encoder(result))
        emitCount += 1
      }
    }
    logger.debug("actor finished processed %d linked tuples, emitted: %d"
      .format(inputs.size, emitCount))
  }
}
