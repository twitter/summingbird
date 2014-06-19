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
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.option.{
  MaxWaitingFutures,
  MaxFutureWaitTime,
  MaxEmitPerExecute
}

class IntermediateFlatMap[T, U, S, D, RC](
    @transient flatMapOp: FlatMapOperation[T, U],
    maxWaitingFutures: MaxWaitingFutures,
    maxWaitingTime: MaxFutureWaitTime,
    maxEmitPerExec: MaxEmitPerExecute,
    pDecoder: Injection[T, D],
    pEncoder: Injection[U, D]) extends AsyncBase[T, U, S, D, RC](maxWaitingFutures, maxWaitingTime, maxEmitPerExec) {

  val encoder = pEncoder
  val decoder = pDecoder

  val lockedOp = Externalizer(flatMapOp)

  override def apply(state: S,
    tup: T): Future[Iterable[(List[S], Future[TraversableOnce[U]])]] =
    lockedOp.get.apply(tup).map { res =>
      List((List(state), Future.value(res)))
    }

  override def cleanup { lockedOp.get.close }
}
