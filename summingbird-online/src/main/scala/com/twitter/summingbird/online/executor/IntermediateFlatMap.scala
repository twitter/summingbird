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

import com.twitter.util.Future

import com.twitter.bijection.Injection
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.option.{
  MaxWaitingFutures,
  MaxFutureWaitTime
}


class IntermediateFlatMap[T,U,S,D](
  @transient flatMapOp: FlatMapOperation[T, U],
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime,
  pDecoder: Injection[(Timestamp, T), D],
  pEncoder: Injection[(Timestamp, U), D]
  ) extends AsyncBase[T,U,S,D](maxWaitingFutures, maxWaitingTime) {

  val encoder = pEncoder
  val decoder = pDecoder

  val lockedOp = Externalizer(flatMapOp)


  override def apply(state: S,
                     timeT: (Timestamp, T)): Future[Iterable[(List[S], Future[TraversableOnce[(Timestamp, U)]])]] =
    lockedOp.get.apply(timeT._2).map { res =>
      List((List(state), Future.value(res.map((timeT._1, _)))))
    }

  override def cleanup { lockedOp.get.close }
}
