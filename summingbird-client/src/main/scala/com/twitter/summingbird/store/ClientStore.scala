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

import com.twitter.algebird.Semigroup
import com.twitter.storehaus.{ FutureCollector, FutureOps, ReadableStore }
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.util.Future
import scala.collection.breakOut

/**
 * Summingbird ClientStore -- merges offline and online.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object ClientStore {
  def apply[K, V](onlineStore: ReadableStore[(K, BatchID), V], batchesToKeep: Int)(implicit batcher: Batcher, semigroup: Semigroup[V]): ClientStore[K, V] =
    apply(ReadableStore.empty, onlineStore, batchesToKeep)

  // If no online store exists, supply an empty store and instruct the
  // client to keep a single batch.
  def apply[K, V](offlineStore: ReadableStore[K, (BatchID, V)])(implicit batcher: Batcher, semigroup: Semigroup[V]): ClientStore[K, V] =
    apply(offlineStore, ReadableStore.empty, 1)

  def defaultOnlineKeyFilter[K] = (k: K) => true

  def apply[K, V](
    offlineStore: ReadableStore[K, (BatchID, V)],
    onlineStore: ReadableStore[(K, BatchID), V],
    batchesToKeep: Int)(implicit batcher: Batcher, semigroup: Semigroup[V]): ClientStore[K, V] =
    new ClientStore[K, V](offlineStore, onlineStore,
      batcher, batchesToKeep, defaultOnlineKeyFilter[K], FutureCollector.bestEffort)

  def apply[K, V](
    offlineStore: ReadableStore[K, (BatchID, V)],
    onlineStore: ReadableStore[(K, BatchID), V],
    batchesToKeep: Int,
    onlineKeyFilter: K => Boolean)(implicit batcher: Batcher, semigroup: Semigroup[V]): ClientStore[K, V] =
    new ClientStore[K, V](offlineStore, onlineStore,
      batcher, batchesToKeep, onlineKeyFilter, FutureCollector.bestEffort)

  def apply[K, V](
    offlineStore: ReadableStore[K, (BatchID, V)],
    onlineStore: ReadableStore[(K, BatchID), V],
    batchesToKeep: Int,
    onlineKeyFilter: K => Boolean,
    collector: FutureCollector[(K, Iterable[BatchID])])(implicit batcher: Batcher, semigroup: Semigroup[V]): ClientStore[K, V] =
    new ClientStore[K, V](offlineStore, onlineStore, batcher, batchesToKeep, onlineKeyFilter, collector)

  /**
   * You can't read the batch counts before what offline has counted up to
   */
  def offlineLTEQBatch[K, V](k: K, b: BatchID, v: Future[Option[(BatchID, V)]]): Future[Option[(BatchID, V)]] =
    v.flatMap {
      case s @ Some((bOld, v)) if (bOld.id <= b.id) => Future.value(s)
      case Some((bOld, v)) => Future.exception(OfflinePassedBatch(k, bOld, b))
      case None => Future.None
    }
}

/**
 * The multiGet uses the "for" syntax internally to chain a bunch of computations
 * from T => Future[U].
 *
 * At a high level, the computation performed by the multiGet is the following:
 *
 *
 * - Look up the set of requested keys in the offlineStore. The
 *   offlineStore holds K -> (BatchID, V).
 *
 * - For each key, use the returned BatchID and the current BatchID
 *   (calculated by the batcher) to generate a sequence of BatchIDs
 *   that the onlineStore is holding. The onlineStore holds (K,
 *   BatchID) -> V, so a join between this BatchID sequence and the
 *   K provides a keyset to use for a multiGet to the onlineStore.
 *
 * - Perform this multiGet to the online store.
 *
 * - PIVOT the BatchIDs out of the online store's key into a
 *   sequence in the value -- then a monoid merge with the offline
 *   store will append the offline value onto the beginning of the
 *   sequence of (BatchID, V).
 *
 * - Finally, reduce this list by monoid-merging together all
 *   (BatchID, V) pairs. If any BatchID is missing from the sequence
 *   (if there are any holes, for example), that particular merged
 *   value's computation in the result will miss the contributions
 *   due to those BatchID's.
 *
 * - Drop the final BatchID off of all successfully aggregated
 *   values (since this BatchID will be the current batch in all
 *   successful cases).
 *
 * The onlineKeyFilter allows only a subset of the keys to be
 * fetched from the realtime layer.  This is a useful optimization
 * in, for example, time series data, where many of the keys that
 * are fetched are historical and therefore only need to be fetched
 * from batch.
 *
 * TODO (https://github.com/twitter/summingbird/issues/72): This
 * filter needs to be generalized correctly, and is probably
 * incorrect at the level of just supplying a boolean function. For
 * example, in most cases a function K => T would help tie in
 * batching logic more easily.
 */
class ClientStore[K, V: Semigroup](
    offlineStore: ReadableStore[K, (BatchID, V)],
    onlineStore: ReadableStore[(K, BatchID), V],
    batcher: Batcher,
    batchesToKeep: Int,
    onlineKeyFilter: K => Boolean,
    collector: FutureCollector[(K, Iterable[BatchID])]) extends ReadableStore[K, V] {
  import MergeOperations._

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, FOpt[V]] =
    multiGetBatch(batcher.currentBatch, ks)

  /*
   * This is a big hint that in fact this store should be a
   * ReadableStore[(K, BatchID), V]
   */
  def multiGetBatch[K1 <: K](batch: BatchID, ks: Set[K1]): Map[K1, FOpt[V]] = {
    val offlineResult: Map[K1, FOpt[(BatchID, V)]] =
      offlineStore.multiGet(ks)
        /*
         * The offline BatchID is an *exclusive* upper bound (see decrementOfflineBatch below).
         * As a result we can just look at the offline store if the that
         * offline batch <= batch.next
         * for the key
         */
        .map { case (k, bv) => (k, ClientStore.offlineLTEQBatch(k, batch.next, bv)) }(breakOut)

    // For combining later we move the offline result batch id from being the exclusive upper bound
    // to the inclusive upper bound.
    val liftedOffline = decrementOfflineBatch(offlineResult)
    val possibleOnlineKeys = ks.filter(onlineKeyFilter)

    /*
     * Mapping from K1 to the first online batch we need to read
     */
    val keyToBatch: K1 => FOpt[BatchID] = offlineResult.andThen(_.map { _.map { _._1 } })

    val fOnlineKeys: Future[Set[(K1, BatchID)]] =
      generateOnlineKeys(possibleOnlineKeys.toSeq, batch, batchesToKeep)(
        keyToBatch)(collector.asInstanceOf[FutureCollector[(K1, Iterable[BatchID])]])

    val m: Future[Map[K1, FOpt[V]]] = fOnlineKeys.map { onlineKeys =>
      val onlineResult: Map[(K1, BatchID), FOpt[V]] = onlineStore.multiGet(onlineKeys)
      val liftedOnline: Map[K1, Future[Seq[Option[(BatchID, V)]]]] = pivotBatches(onlineResult)
      val merged: Map[K1, FOpt[(BatchID, V)]] = mergeResults(liftedOffline, liftedOnline)
      // We discard the BatchID here
      dropBatches(merged)
    }

    FutureOps.liftFutureValues(ks, m)
  }
}

case class OfflinePassedBatch(key: Any, offlineBatch: BatchID, requested: BatchID) extends Exception(s"key: $key offline is at batch $offlineBatch, can't query for $requested")
