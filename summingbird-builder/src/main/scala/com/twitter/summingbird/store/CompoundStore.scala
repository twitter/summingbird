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

import com.twitter.summingbird.online.Externalizer
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.Mergeable
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.batch.BatchedStore

/**
 * Compound BatchStore and Mergeable, used for building a summingbird job.
 *
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class CompoundStore[K, V] private (
  @transient offline: Option[BatchedStore[K, V]],
  online: Option[() => Mergeable[(K, BatchID), V]])
    extends Serializable {
  private val offlineBox = Externalizer(offline)
  def offlineStore: Option[BatchedStore[K, V]] = offlineBox.get
  def onlineSupplier: Option[() => Mergeable[(K, BatchID), V]] = online
}

object CompoundStore {
  def fromOnline[K, V](onlineSupplier: => Mergeable[(K, BatchID), V]): CompoundStore[K, V] =
    new CompoundStore(None, Some(() => onlineSupplier))

  def fromOffline[K, V](store: BatchedStore[K, V]): CompoundStore[K, V] =
    new CompoundStore(Some(store), None)

  def apply[K, V](offlineStore: BatchedStore[K, V], onlineSupplier: => Mergeable[(K, BatchID), V]): CompoundStore[K, V] =
    new CompoundStore(Some(offlineStore), Some(() => onlineSupplier))
}
