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

import com.twitter.algebird.Semigroup
import com.twitter.util.Future

import com.twitter.summingbird.online.Externalizer

import com.twitter.summingbird.online.FlatMapOperation

import com.twitter.summingbird.online.option.{
  SummerBuilder,
  MaxWaitingFutures,
  MaxFutureWaitTime,
  MaxEmitPerExecute
}
import chain.Chain
import scala.collection.mutable.{ Map => MMap, ListBuffer }
// These CMaps we generate in the FFM, we use it as an immutable wrapper around
// a mutable map.
import scala.collection.{ Map => CMap }
import scala.util.control.NonFatal

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 * @author Ian O Connell
 */

// This is not a user settable variable.
// Its supplied by the planning system usually to ensure its large enough to cover the space
// used by the summers times some delta.
private[summingbird] case class KeyValueShards(get: Int) {
  def summerIdFor[K](k: K): Int =
    math.abs(k.hashCode % get)
}

class FinalFlatMap[Event, Key, Value: Semigroup, S <: InputState[_]](
  @transient flatMapOp: FlatMapOperation[Event, (Key, Value)],
  summerBuilder: SummerBuilder,
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  maxEmitPerExec: MaxEmitPerExecute,
  summerShards: KeyValueShards)
    extends AsyncBase[Event, (Int, CMap[Key, Value]), S](maxWaitingFutures,
      maxWaitingTime,
      maxEmitPerExec) {

  type InS = S
  type OutputElement = (Int, CMap[Key, Value])

  val lockedOp = Externalizer(flatMapOp)

  type SummerK = Key
  type SummerV = (Chain[S], Value)

  lazy val sCache = summerBuilder.getSummer[SummerK, SummerV](implicitly[Semigroup[(Chain[S], Value)]])

  // Lazy transient as const futures are not serializable
  @transient private[this] lazy val noData = List(
    (Chain.empty, Future.value(Nil))
  )

  private def formatResult(outData: Map[Key, (Chain[S], Value)]): TraversableOnce[(Chain[S], Future[TraversableOnce[OutputElement]])] = {
    if (outData.isEmpty) {
      noData
    } else {
      var mmMap = MMap[Int, (ListBuffer[S], MMap[Key, Value])]()

      outData.toIterator.foreach {
        case (k, (listS, v)) =>
          val newK = summerShards.summerIdFor(k)
          val (buffer, mmap) = mmMap.getOrElseUpdate(newK, (ListBuffer[S](), MMap[Key, Value]()))
          buffer ++= listS.iterator
          mmap += k -> v
      }

      mmMap.toIterator.map {
        case (outerKey, (listS, innerMap)) =>
          (Chain(listS), Future.value(List((outerKey, innerMap))))
      }
    }
  }

  override def tick: Future[TraversableOnce[(Chain[S], Future[TraversableOnce[OutputElement]])]] =
    sCache.tick.map(formatResult(_))

  def cache(state: S,
    items: TraversableOnce[(Key, Value)]): Future[TraversableOnce[(Chain[S], Future[TraversableOnce[OutputElement]])]] =
    try {
      val itemL = items.toList
      if (itemL.size > 0) {
        state.fanOut(itemL.size)
        sCache.addAll(itemL.map {
          case (k, v) =>
            (k, (Chain.single(state), v))
        }).map(formatResult(_))
      } else { // Here we handle mapping to nothing, option map et. al
        Future.value(
          List(
            (Chain.single(state), Future.value(Nil))
          )
        )
      }
    } catch {
      case NonFatal(e) => Future.exception(e)
    }

  override def apply(state: S,
    tup: Event) =
    lockedOp.get.apply(tup).map { cache(state, _) }.flatten

  override def cleanup(): Unit = {
    lockedOp.get.close
    sCache.cleanup
  }
}
