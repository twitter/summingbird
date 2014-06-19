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

import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.Future

/**
 * MergeableStore that triggers a side effect on every call to put or
 * merge.
 *
 * @author Sam Ritchie
 * @author Oscar Boykin
 */

class SideEffectStore[K, V](store: MergeableStore[K, V])(sideEffectFn: K => Future[Unit])
    extends MergeableStore[K, V] {

  override def semigroup = store.semigroup
  override def get(k: K) = store.get(k)
  override def multiGet[K1 <: K](ks: Set[K1]) = store.multiGet(ks)

  def after[T](t: Future[T])(fn: T => Unit): Future[T] = { t.foreach(fn); t }

  override def put(pair: (K, Option[V])) =
    after(store.put(pair)) { _ => sideEffectFn(pair._1) }

  override def merge(pair: (K, V)) =
    after(store.merge(pair)) { _ => sideEffectFn(pair._1) }
}
