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

package com.twitter.summingbird.storm

import com.twitter.util.Future

import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.storm.option.{
  MaxWaitingFutures,
  MaxFutureWaitTime
}


/**
  * This bolt is used for intermediate flatMapping before a grouping.
  * Its output storm-tuple contains a single scala.Tuple2[Long, U] at
  * the 0 position. The Long is the timestamp of the source object
  * from which U was derived. Each U is one of the output items of the
  * flatMapOp.
  */
class IntermediateFlatMapBolt[T,U,S](
  @transient flatMapOp: FlatMapOperation[T, U],
  maxWaitingFutures: MaxWaitingFutures,
  maxWaitingTime: MaxFutureWaitTime
  ) extends AsyncBaseBolt[T,U,S](maxWaitingFutures, maxWaitingTime) {

  import Constants._
  val lockedOp = Externalizer(flatMapOp)

  override val decoder = new SingleItemInjection[T](VALUE_FIELD)
  override val encoder = new SingleItemInjection[U](VALUE_FIELD)

  override def apply(tup: InputState[S],
                     timeT: (Timestamp, T)): Future[Iterable[(List[InputState[S]], Future[TraversableOnce[(Timestamp, U)]])]] =
    lockedOp.get.apply(timeT._2).map { res =>
      List((List(tup.expand(res.size)), Future.value(res.map((timeT._1, _)))))
    }

  override def cleanup { lockedOp.get.close }
}
