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

package com.twitter.summingbird.scalding.store

import cascading.flow.FlowDef

import com.twitter.scalding.{ Dsl, Mode, TDsl, TypedPipe, IterableSource }
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.ScaldingEnv

import java.io.Serializable

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object BatchReadableStore {
  import Dsl._
  import TDsl._

  def empty[K,V](batchID: BatchID): BatchReadableStore[K,V] =
    new BatchReadableStore[K,V] {
      override def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode) =
        Some((batchID, mappableToTypedPipe(IterableSource(Seq[(K,V)]()))))
      override def read(batchId: BatchID, env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode) = None
      override def availableBatches(env: ScaldingEnv) = Iterable.empty[BatchID]
    }
}

trait BatchReadableStore[K, V] extends Serializable {
  // Return the previous upper bound batchId and the pipe covering this range
  // NONE of the items from the returned batchID should be present in the TypedPipe
  def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode)
  : Option[(BatchID, TypedPipe[(K,V)])]

  /** Read a specific version if it is available */
  def read(batchId: BatchID, env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode): Option[TypedPipe[(K,V)]]

  def availableBatches(env: ScaldingEnv): Iterable[BatchID]
}

trait BatchStore[K,V] extends BatchReadableStore[K,V] with Serializable {
  def write(env: ScaldingEnv, p: TypedPipe[(K,V)])
  (implicit fd: FlowDef, mode: Mode): Unit
  def commit(batchId: BatchID, env: ScaldingEnv): Unit
}
