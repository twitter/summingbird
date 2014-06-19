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

import com.twitter.storehaus.{ ReadableStore, Store }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.batch.BatchID
import com.twitter.util.Future

/**
 * Returns a MergeableStore that augments a Summingbird "online
 * store" with the ability to snapshot keys from the combined
 * online/offline ClientStore into some third presenting store every
 * time a key is touched.
 *
 * TODO (https://github.com/twitter/summingbird/issues/73): Write a
 * version of this that accepts the most general presenting function:
 * (K, V) => Iterable[(K2, V2)] and move to Storehaus.
 *
 * @author Sam Ritchie
 * @author Oscar Boykin
 */
object PresentingStore {
  def apply[K, V, U](
    onlineStore: MergeableStore[(K, BatchID), V],
    clientStore: ReadableStore[K, V],
    presentingStore: Store[K, U])(fn: V => U): MergeableStore[(K, BatchID), V] =
    new SideEffectStore(onlineStore)({
      case (k, _) =>
        clientStore.get(k).flatMap { optV =>
          presentingStore.put(k -> optV.map(fn))
        }
    })
}
