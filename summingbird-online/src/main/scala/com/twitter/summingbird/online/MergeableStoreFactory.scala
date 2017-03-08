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

import com.twitter.algebird.Semigroup
import com.twitter.storehaus.algebra.{ MergeableStore, Mergeable }
import com.twitter.summingbird.batch.{ Batcher, BatchID }

/*
 * A MergeableStoreFactory is used in online jobs where we need a means to create a storehaus store, and have a batcher for that store.
 *
 * We use Function1 since it makes daisy chaining operations much cleaner with .andThen
 */
object MergeableStoreFactory {

  def apply[K, V](store: () => Mergeable[K, V], batcher: Batcher) =
    fromWithSemigroup { _: Semigroup[V] => store() }(batcher)

  def from[K, V](store: => Mergeable[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreFactory[(K, BatchID), V] =
    fromWithSemigroup(_ => store)

  def fromWithSemigroup[K, V](fn: Semigroup[V] => Mergeable[K, V])(implicit batcher: Batcher): MergeableStoreFactory[K, V] =
    new MergeableStoreFactory[K, V] {
      def mergeableStore = fn
      def mergeableBatcher = batcher
    }

  def fromOnlineOnly[K, V](store: => MergeableStore[K, V]): MergeableStoreFactory[(K, BatchID), V] =
    fromOnlineOnlyWithSemigroup(_ => store)

  def fromOnlineOnlyWithSemigroup[K, V](fn: Semigroup[V] => MergeableStore[K, V]): MergeableStoreFactory[(K, BatchID), V] =
    fromWithSemigroup(fn.andThen(_.convert { k: (K, BatchID) => k._1 }))(Batcher.unit)
}

trait MergeableStoreFactory[-K, V] extends java.io.Serializable {
  def mergeableStore: Semigroup[V] => Mergeable[K, V]
  def mergeableBatcher: Batcher
}
