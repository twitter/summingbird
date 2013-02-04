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

import com.twitter.algebird.Monoid
import com.twitter.bijection.Pivot
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future

/**
 * Merged Store for use in Summingbird clients.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

object ClientStore {
  def apply[Time: Batcher, Key, Value: Monoid]
  (onlineStore: ReadableStore[(Key, BatchID), Value], batchesToKeep: Int): ClientStore[Time, Key, Value] =
    apply(ReadableStore.empty[Key, (BatchID, Value)], onlineStore, batchesToKeep)

  // If no online store exists, supply an empty store and instruct the
  // client to keep a single batch.
  def apply[Time: Batcher, Key, Value: Monoid]
  (offlineStore: ReadableStore[Key, (BatchID, Value)]): ClientStore[Time, Key, Value] =
    apply(offlineStore, ReadableStore.empty[(Key, BatchID), Value], 1)

  def apply[Time, Key, Value](offlineStore: ReadableStore[Key, (BatchID, Value)],
                              onlineStore: ReadableStore[(Key, BatchID), Value],
                              batchesToKeep: Int)
  (implicit batcher: Batcher[Time], monoid: Monoid[Value]): ClientStore[Time, Key, Value] =
    new ClientStore[Time, Key, Value](offlineStore, onlineStore, batcher, monoid, batchesToKeep)
}

class ClientStore[Time, Key, Value](offlineStore: ReadableStore[Key, (BatchID, Value)],
                                    onlineStore: ReadableStore[(Key, BatchID), Value],
                                    batcher: Batcher[Time],
                                    monoid: Monoid[Value],
                                    batchesToKeep: Int)
extends ReadableStore[Key, Value] {
  implicit val m: Monoid[Value] = monoid

  // Pivot to pivot (Key,BatchID) pairs into K -> (BatchID,V).
  // TODO: rewrite the timeOf method in the batcher in terms of the pivot.
  val pivot = Pivot.of[(Key, BatchID), Key, BatchID].withValue[Value]

  protected def defaultInit(nowBatch: BatchID = batcher.currentBatch)
  : Option[(BatchID,Value)] =
    Some((nowBatch - (batchesToKeep - 1), Monoid.zero))

  protected def expand(init: Option[(BatchID,Value)],
                       nowBatch: BatchID = batcher.currentBatch)
  : Iterable[(BatchID,Value)] = {
    val (initBatch,initV) = Monoid.plus(defaultInit(nowBatch), init).get
    BatchID.range(initBatch, nowBatch)
    .map { batchID => (batchID, initV) }
    .toIterable
  }

  // Returns an iterable of all possible (Key,BatchID) combinations for
  // a given input map `m`. The type of `m` matches the return type of
  // multiOfflineGet.
  protected def generateOnlineKeys(ks: Iterable[Key], nowBatch: BatchID)
  (lookup: (Key) => Option[(BatchID, Value)])
  : Set[(Key, BatchID)] =
    pivot.invert(ks.map { k: Key => (k -> expand(lookup(k), nowBatch)) }.toMap)
  .map { pair: ((Key,BatchID),Value) => pair._1 }.toSet

  private def sortedSum(it: Iterable[(BatchID,Value)]): (BatchID,Value) =
    Monoid.sum(it.toList.sortBy { _._1 })

  override def multiGet(ks: Set[Key]): Future[Map[Key,Value]] =
    for (offlineResult <- offlineStore.multiGet(ks); // already packed.
         onlineKeys = generateOnlineKeys(ks, batcher.currentBatch) { offlineResult.get(_) };
         onlineRet <- onlineStore.multiGet(onlineKeys); // unpacked!
         onlineResult = pivot(onlineRet) mapValues { sortedSum(_) })
    yield Monoid.plus(offlineResult, onlineResult) mapValues { _._2 }
}
