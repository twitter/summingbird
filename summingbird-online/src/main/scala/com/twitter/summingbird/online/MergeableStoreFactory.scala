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

package com.twitter.summingbird.online

import com.twitter.storehaus.algebra.{ MergeableStore, Mergeable, StoreAlgebra }
import com.twitter.summingbird.batch.{ Batcher, BatchID }

object MergeableStoreFactory {

  def from[K, V](store: => Mergeable[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreFactory[K, V] =
    MergeableStoreFactory({ (_: Unit) => store }, batcher)

  def fromOnlineOnly[K, V](store: => MergeableStore[K, V]): MergeableStoreFactory[K, V] = {
    implicit val batcher = Batcher.unit
    from(store.convert { k: (K, BatchID) => k._1 })
  }
}

case class MergeableStoreFactory[-K, V](
  store: Function1[Unit, Mergeable[(K, BatchID), V]],
  batcher: Batcher)
