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

import com.twitter.algebird.{ Monoid, Semigroup, SeqMonoid }
import com.twitter.algebird.util.UtilAlgebras._
import com.twitter.bijection.Pivot
import com.twitter.storehaus.{ FutureCollector, FutureOps, ReadableStore }
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.util.{ Future, Return, Throw, Try }

/**
 * Summingbird ClientStore -- merges offline and online.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object ClientStore {
  def apply[K, V](onlineStore: ReadableStore[(K, BatchID), V], batchesToKeep: Int)
    (implicit batcher: Batcher, monoid: Monoid[V]): ClientStore[K, V] =
    apply(ReadableStore.empty, onlineStore, batchesToKeep)

  // If no online store exists, supply an empty store and instruct the
  // client to keep a single batch.
  def apply[K, V](offlineStore: ReadableStore[K, (BatchID, V)])
    (implicit batcher: Batcher, monoid: Monoid[V]): ClientStore[K, V] =
    apply(offlineStore, ReadableStore.empty, 1)

  def defaultOnlineKeyFilter[K] = (k: K) => true

  def apply[K, V](
    offlineStore: ReadableStore[K, (BatchID, V)],
    onlineStore: ReadableStore[(K, BatchID), V],
    batchesToKeep: Int,
    onlineKeyFilter: K => Boolean = defaultOnlineKeyFilter[K]
  )(implicit batcher: Batcher, monoid: Monoid[V]): ClientStore[K, V] =
    new ClientStore[K, V](offlineStore, onlineStore, batcher, monoid, batchesToKeep, onlineKeyFilter)
}

object MergeOperations {
  // TODO (https://github.com/twitter/summingbird/issues/71): the
  // check here on sequential batch IDs was wrong. we should be
  // checking that the batch layer is not more than batchesToKeep
  // behind. the manner of doing that on a per-key basis will change
  // in storehaus 0.3, so add in that check during the upgrade.
  def sortedSum[V: Semigroup](opts: Seq[Option[(BatchID, V)]])
      : Try[Option[(BatchID, V)]] = {
    val sorted = opts.flatten.sortBy {  _._1 }
    Return(Semigroup.sumOption(sorted))
  }

  /**
   * Pivot to pivot (K, BatchID) pairs into K -> (BatchID, V).
   */
  def pivot[K, V] = Pivot.of[(K, BatchID), K, BatchID].withValue[V]

  def collect[T, U](seq: Seq[(T, Future[U])]): Future[Seq[(T, U)]] =
    Future.collect(seq map { case (t, futureU) => futureU map { (t, _) } })

  def mergeResults[K, V: Monoid](m1: Map[K, Future[Seq[Option[(BatchID, V)]]]],
                                 m2: Map[K, Future[Seq[Option[(BatchID, V)]]]])
  : Map[K, Future[Option[(BatchID, V)]]] =
    Monoid.plus(m1, m2) mapValues { _.flatMap { opts => Future.const(sortedSum(opts)) } }

  def dropBatches[K, V](m: Map[K, Future[Option[(BatchID, V)]]]): Map[K, Future[Option[V]]] =
    m mapValues { _.map { _.map { _._2 } } }
}

class ClientStore[K, V](
  offlineStore: ReadableStore[K, (BatchID, V)],
  onlineStore: ReadableStore[(K, BatchID), V],
  batcher: Batcher,
  monoid: Monoid[V],
  batchesToKeep: Int,
  onlineKeyFilter: K => Boolean
) extends ReadableStore[K, V] {
  import MergeOperations._

  type FOpt[T] = Future[Option[T]]

  implicit val implicitMonoid: Monoid[V] = monoid

  /**
    * If offlineStore has no return value for some key,
    * defaultOfflineReturn usees batchesToKeep to calculate a
    * reasonable beginning batchID from which to start querying the
    * onlineStore.
    */
  protected def defaultOfflineReturn(nowBatch: BatchID): Option[(BatchID, V)] =
    Some(nowBatch - (batchesToKeep - 1), Monoid.zero[V])

  protected def expand(offlineReturn: Option[(BatchID, V)], nowBatch: BatchID): Iterable[(BatchID, V)] = {
    val (initBatch,initV) = Monoid.plus(defaultOfflineReturn(nowBatch), offlineReturn).get
    BatchID.range(initBatch, nowBatch)
      .map { batchID => (batchID, initV) }
  }

  protected def generateOnlineKeys[K1 <: K](ks: Seq[K1], nowBatch: BatchID)(lookup: K1 => FOpt[(BatchID, V)]): Future[Set[(K1, BatchID)]] = {
    val futures: Seq[Future[(K1, Iterable[(BatchID, V)])]] =
      ks.map { k: K1 => lookup(k) map { (k -> expand(_, nowBatch)) } }
    for {
      collected <- FutureCollector.bestEffort(futures)
      inverted: Iterable[((K1, BatchID), V)] = pivot.invert(collected.toMap)
    } yield inverted.map { pair: ((K1, BatchID), V) => pair._1 }.toSet
  }

  /**
   * Pivots each BatchID out of the key and into the Value's future.
   */
  protected def pivotBatches[K1 <: K](m: Map[(K1, BatchID), FOpt[V]]): Map[K1, Future[Seq[Option[(BatchID, V)]]]] =
    pivot(m).mapValues { it =>
      collect(it.toSeq).map { _.map { case (batchID, optV) => optV map { (batchID, _) } } }
    }

  def decrementOfflineBatch[K1 <: K](m: Map[K1, FOpt[(BatchID, V)]]): Map[K1, Future[Seq[Option[(BatchID, V)]]]] =
    m.mapValues { futureOptV =>
      futureOptV.map { optV =>
        Seq(optV.map { case (batchID, v) => (batchID.prev, v) })
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
   *   value's computation in the result will be a Future.exception vs
   *   a defined future.
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

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, FOpt[V]] = {
    val offlineResult: Map[K1, FOpt[(BatchID, V)]] = offlineStore.multiGet(ks)
    val liftedOffline = decrementOfflineBatch(offlineResult)
    val possibleOnlineKeys = ks.filter(onlineKeyFilter)
    val m: Future[Map[K1, FOpt[V]]] = for (
      onlineKeys <- generateOnlineKeys(possibleOnlineKeys.toSeq, batcher.currentBatch)(offlineResult);
      onlineResult: Map[(K1, BatchID), FOpt[V]] = onlineStore.multiGet(onlineKeys);
      liftedOnline: Map[K1, Future[Seq[Option[(BatchID, V)]]]] = pivotBatches(onlineResult)
    ) yield dropBatches(mergeResults(liftedOffline, liftedOnline))
    FutureOps.liftFutureValues(ks, m)
  }
}
