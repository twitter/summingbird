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
import com.twitter.summingbird.batch.{ Batcher, BatchID, Timestamp}
import com.twitter.summingbird.online.{FlatMapOperation, MultiTriggerCache}
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.online.option.{
  MaxWaitingFutures,
  MaxFutureWaitTime,
  FlushFrequency
}


/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 * @author Ian O Connell
 */

class FinalFlatMap[Event, Key, Value, InputWireFmnt, OutputWireFmnt](
  @transient flatMapOp: FlatMapOperation[Event, (Key, Value)],
  cacheSize: CacheSize,
  flushFrequency: FlushFrequency,
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  pDecoder: DataSer[Event, InputWireFmnt],
  pEncoder: DataSer[((Key, BatchID), Value), OutputWireFmnt]
  )
  (implicit monoid: Semigroup[Value], batcher: Batcher)
    extends AsyncBase[Event, ((Key, BatchID), Value), InputWireFmnt, OutputWireFmnt](maxWaitingFutures,
                                                          maxWaitingTime) {
  val encoder = pEncoder
  val decoder = pDecoder

  val lockedOp = Externalizer(flatMapOp)
  lazy val sCache: MultiTriggerCache[(Key, BatchID), (List[InputState[InputWireFmnt]], Timestamp, Value)] = new MultiTriggerCache(cacheSize, flushFrequency)

  private def formatResult(outData: Map[(Key, BatchID), (List[InputState[InputWireFmnt]], Timestamp, Value)])
                        : Iterable[(List[InputState[InputWireFmnt]], Future[TraversableOnce[(Timestamp, ((Key, BatchID), Value))]])] = {

    outData.toList.map{ case ((key, batchID), (tupList, ts, value)) =>
      (tupList, Future.value(List((ts, ((key, batchID), value)))))
    }
  }

  override def tick: Future[Iterable[(List[InputState[InputWireFmnt]], Future[TraversableOnce[(Timestamp, ((Key, BatchID), Value))]])]] = {
    sCache.tick.map(formatResult(_))
  }

  def cache(tuple: InputState[InputWireFmnt],
            time: Timestamp,
            items: TraversableOnce[(Key, Value)]): Future[Iterable[(List[InputState[InputWireFmnt]], Future[TraversableOnce[(Timestamp, ((Key, BatchID), Value))]])]] = {

    val batchID = batcher.batchOf(time)
    val itemL = items.toList
    sCache.insert(itemL.map{case (k, v) => (k, batchID) -> (List(tuple.expand(1)), time, v)}).map(formatResult(_))
  }

  override def apply(tup: InputState[InputWireFmnt],
                     timeIn: (Timestamp, Event)) =
    lockedOp.get.apply(timeIn._2).map { cache(tup, timeIn._1, _) }.flatten

  override def cleanup { lockedOp.get.close }
}
