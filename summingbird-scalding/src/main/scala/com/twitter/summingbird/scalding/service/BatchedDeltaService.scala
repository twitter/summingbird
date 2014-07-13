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

package com.twitter.summingbird.scalding.service

import com.twitter.algebird.monad.{ StateWithError, Reader }
import com.twitter.algebird.Semigroup
import com.twitter.scalding.{ Mode, TypedPipe }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import cascading.flow.FlowDef
import com.twitter.summingbird.scalding._

/**
 * Use this when you have written JUST BEFORE the store.
 * This is what you get from an IntermediateWrite in Builder API.
 */
class BatchedDeltaService[K, V: Semigroup](val store: batch.BatchedStore[K, V],
    val deltas: batch.BatchedSink[(K, V)],
    override val reducers: Option[Int] = None) extends batch.BatchedService[K, V] {

  assert(store.batcher == deltas.batcher, "Batchers do not match")

  override def ordering = store.ordering
  override def batcher = store.batcher
  override def readStream(batchID: BatchID, mode: Mode) =
    deltas.readStream(batchID, mode).map { fp =>
      fp.map { pipe =>
        pipe.mapValues { kv: (K, V) => (kv._1, Some(kv._2)) } // case confused compiler here
      }
    }
  override def readLast(exclusiveUB: BatchID, mode: Mode) =
    store.readLast(exclusiveUB, mode)

  /**
   * This executes the join algorithm on the streams.
   * You are guaranteed that all the service data needed
   * to do the join is present.
   */
  override def lookup[W](incoming: TypedPipe[(Timestamp, (K, W))],
    servStream: TypedPipe[(Timestamp, (K, Option[V]))]): TypedPipe[(Timestamp, (K, (W, Option[V])))] = {

    def flatOpt[T](o: Option[Option[T]]): Option[T] = o.flatMap(identity)

    implicit val ord = ordering
    LookupJoin.rightSumming(incoming, servStream, reducers)
      .map { case (t, (k, (w, optoptv))) => (t, (k, (w, flatOpt(optoptv)))) }
  }
}
