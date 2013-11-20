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

import com.twitter.summingbird.online.Externalizer
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.akka.option.MaxWaitingFutures
import com.twitter.summingbird.online.FlatMapOperation

import com.twitter.util.{Future}

import java.util.{ Date, Arrays => JArrays, List => JList, Map => JMap, ArrayList => JAList }

/**
  * This bolt is used for intermediate flatMapping before a grouping.
  * Its output storm-tuple contains a single scala.Tuple2[Long, U] at
  * the 0 position. The Long is the timestamp of the source object
  * from which U was derived. Each U is one of the output items of the
  * flatMapOp.
  */
class IntermediateFlatMapActor[T,U](
  @transient flatMapOp: FlatMapOperation[T, U],
  maxWaitingFutures: MaxWaitingFutures,
  shouldEmit: Boolean,
  targetNames: List[String]) extends
    AsyncBaseActor[T,U](maxWaitingFutures, shouldEmit, targetNames) {

  val lockedOp = Externalizer(flatMapOp)

  override val decoder = new SingleItemInjection[T]
  override val encoder = new SingleItemInjection[U]

  override def apply(tup: WireType[T],
                     timeT: (Timestamp, T)) =
    Future.value(List((JArrays.asList(tup),
      lockedOp.get.apply(timeT._2).map { res => res.map((timeT._1, _)) })))

  // override def cleanup { lockedOp.get.close }
}
