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

import com.twitter.algebird.{SummingQueue, Semigroup, MapAlgebra}
import com.twitter.algebird.util.summer.AsyncSummer
import com.twitter.bijection.Injection
import com.twitter.util.Future

import com.twitter.summingbird.online.Externalizer

import com.twitter.summingbird.online.FlatMapOperation

import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.online.option.{
  SummerBuilder,
  MaxWaitingFutures,
  MaxFutureWaitTime,
  MaxEmitPerExecute,
  FlushFrequency
}


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
  def summerIdFor[K](k: K): Int = k.hashCode % get
}

class FinalFlatMap[Event, Key, Value: Semigroup, S <: InputState[_], D, RC](
  @transient flatMapOp: FlatMapOperation[Event, (Key, Value)],
  summerBuilder: SummerBuilder,
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  maxEmitPerExec: MaxEmitPerExecute,
  summerShards: KeyValueShards,
  pDecoder: Injection[Event, D],
  pEncoder: Injection[(Int, Map[Key, Value]), D]
  )
  extends AsyncBase[Event, (Int, Map[Key, Value]), S, D, RC](maxWaitingFutures,
                                                          maxWaitingTime,
                                                          maxEmitPerExec) {

  type InS = S
  type OutputElement = (Int, Map[Key, Value])

  val encoder = pEncoder
  val decoder = pDecoder

  val lockedOp = Externalizer(flatMapOp)

  type SummerK = Int
  type SummerV = (List[S], Map[Key, Value])
  lazy val sCache = summerBuilder.getSummer[SummerK, SummerV](implicitly[Semigroup[(List[S], Map[Key, Value])]])

  private def formatResult(outData: Map[Int, (List[S], Map[Key, Value])])
                        : TraversableOnce[(List[S], Future[TraversableOnce[OutputElement]])] = {
    outData.iterator.map { case (outerKey, (tupList, valList)) =>
      if(valList.isEmpty) {
        (tupList, Future.value(Nil))
      } else {
        (tupList, Future.value(List((outerKey, valList))))
      }
    }
  }

  override def tick: Future[TraversableOnce[(List[S], Future[TraversableOnce[OutputElement]])]] = {
    sCache.tick.map(formatResult(_))
  }

  def cache(state: S,
            items: TraversableOnce[(Key, Value)]): Future[TraversableOnce[(List[S], Future[TraversableOnce[OutputElement]])]] = {
    try {
      val itemL = items.toList
      if(itemL.size > 0) {
        state.fanOut(itemL.size)
        sCache.addAll(itemL.map{case (k, v) =>
          summerShards.summerIdFor(k) -> (List(state), Map(k -> v))
        }).map(formatResult(_))
      }
      else { // Here we handle mapping to nothing, option map et. al
          Future.value(
            List(
              (List(state), Future.value(Nil))
            )
          )
        }
    }
    catch {
      case t: Throwable => Future.exception(t)
    }
  }

  override def apply(state: S,
                     tup: Event) =
    lockedOp.get.apply(tup).map { cache(state, _) }.flatten

  override def cleanup {
    lockedOp.get.close
    sCache.cleanup
  }
}
