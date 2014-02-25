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
import com.twitter.bijection.Injection
import com.twitter.util.Future

import com.twitter.summingbird.online.Externalizer

import com.twitter.summingbird.online.{FlatMapOperation, AsyncCache, CacheBuilder}
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.online.option.{
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

case class KeyValueShards(get: Int)

class FinalFlatMap[Event, Key, Value: Semigroup, S, D](
  @transient flatMapOp: FlatMapOperation[Event, (Key, Value)],
  cacheBuilder: CacheBuilder[Int, (Key, (List[InputState[S]], Value))],
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  maxEmitPerExec: MaxEmitPerExecute,
  summerShards: KeyValueShards,
  pDecoder: Injection[Event, D],
  pEncoder: Injection[(Int, List[(Key, Value)]), D]
  )
    extends AsyncBase[Event, (Int, List[(Key, Value)]), InputState[S], D](maxWaitingFutures,
                                                          maxWaitingTime,
                                                          maxEmitPerExec) {

  type InS = InputState[S]
  type OutputElement = (Int, List[(Key, Value)])

  val encoder = pEncoder
  val decoder = pDecoder

  val lockedOp = Externalizer(flatMapOp)
  val sg = new Semigroup[(List[InS], Map[Key, Value])] {
    type InLists = (List[InS], Map[Key, Value])
    def plus(a: InLists, b: InLists): InLists = {
      val tups = a._1 ++ b._1
      (tups, Semigroup.plus(a._2, b._2))
    }

    override def sumOption(iter: TraversableOnce[InLists]): Option[InLists] = {
      val seqV = iter.toSeq
      for {
        a <- Semigroup.sumOption(seqV.map(_._1))
        b <- Semigroup.sumOption(seqV.map(_._2))
      } yield (a, b)
    }
  }

  lazy val sCache = cacheBuilder(sg)

  private def formatResult(outData: Map[Int, (List[InputState[S]], Map[Key, Value])])
                        : TraversableOnce[(List[InputState[S]], Future[TraversableOnce[OutputElement]])] = {
    outData.iterator.map { case (outerKey, (tupList, valList)) =>
      if(valList.isEmpty) {
        (tupList, Future.value(Nil))
      } else {
        (tupList, Future.value(List((outerKey, valList.toList))))
      }
    }
  }

  override def tick: Future[TraversableOnce[(List[InputState[S]], Future[TraversableOnce[OutputElement]])]] = {
    sCache.tick.map(formatResult(_))
  }

  def cache(state: InputState[S],
            items: TraversableOnce[(Key, Value)]): Future[TraversableOnce[(List[InputState[S]], Future[TraversableOnce[OutputElement]])]] = {

    val itemL = items.toList
    if(itemL.size > 0) {
      state.fanOut(itemL.size - 1) // Since input state starts at a 1
      sCache.insert(itemL.map{case (k, v) =>
        (k.hashCode % summerShards.get) -> (List(state), Map(k -> v))
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

  override def apply(state: InputState[S],
                     tup: Event) =
    lockedOp.get.apply(tup).map { cache(state, _) }.flatten

  override def cleanup {
    lockedOp.get.close
    sCache.cleanup
  }
}
