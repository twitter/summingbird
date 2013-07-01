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
import com.twitter.bijection.Injection
import com.twitter.scalding.{Dsl, Mode, TDsl, TypedPipe}
import com.twitter.scalding.commons.source.VersionedKeyValSource
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.{ ScaldingEnv, VersionedBatchStore }
import scala.util.control.Exception.allCatch

/**
 * Scalding implementation of the batch read and write components
 * of a store that uses the VersionedKeyValSource from scalding-commons.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object VersionedStore {
  /**
    * Returns a VersionedBatchStore that tags the BatchID alongside
    * the stored value. This is required to serve data through a
    * read-only key-value store designed to serve values in tandem
    * with a realtime layer (that stores (K, BatchID) -> V). See
    * summingbird-client's ClientStore for more information.
    */
  def apply[K, V](rootPath: String, versionsToKeep: Int = VersionedKeyValSource.defaultVersionsToKeep)
    (implicit injection: Injection[(K, (BatchID, V)), (Array[Byte], Array[Byte])],
      batcher: Batcher,
      ord: Ordering[K]): VersionedBatchStore[K, V, K, (BatchID, V)] =
    new VersionedBatchStore[K, V, K, (BatchID, V)](
      rootPath, versionsToKeep, batcher
    )({ case (batchID, (k, v)) => (k, (batchID, v)) })({ case (k, (batchID, v)) => (k, v) })
}
