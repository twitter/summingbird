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

package com.twitter.summingbird.store

import com.twitter.chill.MeatLocker
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.store.BatchStore

/**
 * Compound BatchStore and MergeableStore, used for building a summingbird job.
 *
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class CompoundStore[K, V] private (@transient offline: Option[BatchStore[K, (BatchID, V)]], online: Option[() => MergeableStore[(K, BatchID), V]])
    extends Serializable {
  // MeatLocker these to protect them from serialization errors.
  private val offlineBox = offline.map { MeatLocker(_) }
  def offlineStore: BatchStore[K, (BatchID, V)] = offlineBox.get.get
  def onlineSupplier: () => MergeableStore[(K, BatchID), V] = online.get
}

object CompoundStore {
  def fromOnline[K, V](onlineSupplier: => MergeableStore[(K, BatchID), V]): CompoundStore[K, V] =
    new CompoundStore(None, Some(() => onlineSupplier))

  def fromOffline[K, V](store: BatchStore[K, (BatchID, V)]): CompoundStore[K, V] =
    new CompoundStore(Some(store), None)

  def apply[K, V](offlineStore: BatchStore[K, (BatchID, V)], onlineSupplier: => MergeableStore[(K, BatchID), V]): CompoundStore[K, V] =
    new CompoundStore(Some(offlineStore), Some(() => onlineSupplier))
}
