/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.store

import cascading.flow.FlowDef

import com.twitter.chill.MeatLocker
import com.twitter.scalding.{ Mode, TypedPipe }
import com.twitter.storehaus.{ ReadableStore, Store }
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.scalding.store.{ BatchReadableStore, BatchStore, EmptyBatchStore }
import com.twitter.util.Future

/**
 * Compound BatchStore and Store, used for building a summingbird job.
 *
 * @author Sam Ritchie
 */

class CompoundStore[StoreType <: Store[StoreType, (K, BatchID), V], K, V]
(@transient offline: BatchStore[K, (BatchID, V)], @transient online: StoreType) {
  // MeatLocker these to protect them from serialization errors.
  val offlineBox = MeatLocker(offline)
  val onlineBox = MeatLocker(online)

  def offlineStore: BatchStore[K, (BatchID, V)] = offlineBox.get
  def onlineStore: StoreType = onlineBox.get
}



object CompoundStore {
  // These implicits call directly through to the overloaded "apply"
  // method because
  // 2) We wanted to have the 1-arity version of "apply" accept either online or offline
  // 3) overloaded implicit methods (two implicit "apply"s, for example) aren't allowed.

  implicit def fromOnline[StoreType <: Store[StoreType, (K, BatchID), V], K, V](store: StoreType)
  : CompoundStore[StoreType, K, V] = apply(store)

  implicit def fromOffline[K, V](store: BatchStore[K, (BatchID, V)]): CompoundStore[EmptyStore[(K, BatchID), V], K, V] = apply(store)

  def apply[StoreType <: Store[StoreType, (K, BatchID), V], K, V]
  (offlineStore: BatchStore[K, (BatchID, V)], onlineStore: StoreType): CompoundStore[StoreType, K, V] =
    new CompoundStore[StoreType, K, V](offlineStore, onlineStore)

  def apply[StoreType <: Store[StoreType, (K, BatchID), V], K, V](store: StoreType): CompoundStore[StoreType, K, V] =
    apply(new EmptyBatchStore[K, (BatchID, V)], store)

  def apply[K, V](store: BatchStore[K, (BatchID, V)]): CompoundStore[EmptyStore[(K, BatchID), V], K, V] =
    apply(store, new EmptyStore[(K, BatchID), V])
}

class EmptyStore[K, V] extends Store[EmptyStore[K, V], K, V] {
  lazy val empty = ReadableStore.empty[K,V]
  override def get(k: K) = empty.get(k)
  override def multiGet(ks: Set[K]) = empty.multiGet(ks)
  override def -(k: K) = sys.error("Can't call - in offline mode.")
  override def +(pair: (K,V)) = sys.error("Can't call + in offline mode.")
}
