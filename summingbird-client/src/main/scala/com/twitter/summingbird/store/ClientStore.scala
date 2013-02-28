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
import com.twitter.bijection.Pivot
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.Algebras
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
  def apply[T: Batcher, K, V: Monoid]
  (onlineStore: ReadableStore[(K, BatchID), V], batchesToKeep: Int): ClientStore[T, K, V] =
    apply(ReadableStore.empty[K, (BatchID, V)], onlineStore, batchesToKeep)

  // If no online store exists, supply an empty store and instruct the
  // client to keep a single batch.
  def apply[T: Batcher, K, V: Monoid]
  (offlineStore: ReadableStore[K, (BatchID, V)]): ClientStore[T, K, V] =
    apply(offlineStore, ReadableStore.empty[(K, BatchID), V], 1)

  def apply[T, K, V](offlineStore: ReadableStore[K, (BatchID, V)],
                     onlineStore: ReadableStore[(K, BatchID), V],
                     batchesToKeep: Int)
  (implicit batcher: Batcher[T], monoid: Monoid[V]): ClientStore[T, K, V] =
    new ClientStore[T, K, V](offlineStore, onlineStore, batcher, monoid, batchesToKeep)
}

class MissingBatchException(msg: String) extends RuntimeException(msg)

object MergeOperations {
  import Algebras._ // import Future monoid

  /**
   * Returns true if every pair in the sequence returns true
   * for areConsecutive, false otherwise.
   */
  def isSequential[T](seq: Seq[T])(areConsecutive: (T, T) => Boolean): Boolean =
    seq.sliding(2).forall { l =>
      l match {
        case Seq(a, b) => areConsecutive(a, b)
        case Seq(_) | Seq() => true
      }
    }

  // TODO: Once we upgrade past util 6, replace this with Future.const.
  def const[T](result: Try[T]): Future[T] =
    result match {
      case Return(v) => Future.value(v)
      case Throw(throwable) => Future.exception(throwable)
    }

  def sortedSum[V: Semigroup](opts: Seq[Option[(BatchID, V)]]): Try[Option[(BatchID, V)]] = {
    val sorted = opts.flatten.sortBy {  _._1 }
    if (isSequential(sorted) { (pair1, pair2) => pair1._1.next == pair2._1 })
      Return(Semigroup.sumOption(sorted))
    else
      Throw(new MissingBatchException("Missing BatchIDs: " + sorted.map { _._1 }.mkString(", ")))
  }

  /**
   * Pivot to pivot (K, BatchID) pairs into K -> (BatchID, V).
   * TODO: rewrite the timeOf method in the batcher in terms of the pivot.
   */
  def pivot[K, V] = Pivot.of[(K, BatchID), K, BatchID].withValue[V]

  def collect[T, U](seq: Seq[(T, Future[U])]): Future[Seq[(T, U)]] =
    Future.collect(seq map { case (t, futureU) => futureU map { (t, _) } })

  // TODO (sritchie): Not sure why, but this isn't coming in from the
  // companion object. Try deleting on algebird 0.1.9 upgrade.
  implicit def seqMonoid[V]: Monoid[Seq[V]] = new SeqMonoid[V]

  def mergeResults[K, V: Monoid](m1: Map[K, Future[Seq[Option[(BatchID, V)]]]],
                                 m2: Map[K, Future[Seq[Option[(BatchID, V)]]]])
  : Map[K, Future[Option[(BatchID, V)]]] =
    Monoid.plus(m1, m2) mapValues { _.flatMap { opts => const(sortedSum(opts)) } }

  def dropBatches[K, V](m: Map[K, Future[Option[(BatchID, V)]]]): Map[K, Future[Option[V]]] =
    m mapValues { _.map { _.map { _._2 } } }
}

class ClientStore[T, K, V](offlineStore: ReadableStore[K, (BatchID, V)],
                           onlineStore: ReadableStore[(K, BatchID), V],
                           batcher: Batcher[T],
                           monoid: Monoid[V],
                           batchesToKeep: Int)
extends ReadableStore[K, V] {
  import MergeOperations._

  implicit val implicitMonoid: Monoid[V] = monoid

  protected def defaultInit(nowBatch: BatchID = batcher.currentBatch): Option[(BatchID, V)] =
    Some((nowBatch - (batchesToKeep - 1), Monoid.zero[V]))

  protected def expand(init: Option[(BatchID, V)], nowBatch: BatchID = batcher.currentBatch)
  : Iterable[(BatchID, V)] = {
    val (initBatch,initV) = Monoid.plus(defaultInit(nowBatch), init).get
    BatchID.range(initBatch, nowBatch)
      .map { batchID => (batchID, initV) }
      .toIterable
  }

  protected def generateOnlineKeys(ks: Seq[K], nowBatch: BatchID)(lookup: K => Future[Option[(BatchID, V)]])
  : Future[Set[(K, BatchID)]] = {
    val futures: Seq[Future[(K, Iterable[(BatchID, V)])]] =
      ks.map { k: K => lookup(k) map { (k -> expand(_, nowBatch)) } }
    for (collected <- Future.collect(futures);
         inverted: Iterable[((K, BatchID), V)] = pivot.invert(collected.toMap))
    yield inverted.map { pair: ((K, BatchID), V) => pair._1 }.toSet
  }

  /**
   * Pivots each BatchID out of the key and into the Value's future.
   */
  protected def pivotBatches(m: Map[(K, BatchID), Future[Option[V]]]): Map[K, Future[Seq[Option[(BatchID, V)]]]] =
    pivot(m).mapValues { it =>
      collect(it.toSeq).map { _.map { case (batchID, optV) => optV map { (batchID, _) } } }
    }

  /**
   * The multiGet uses the "for" syntax internally to chain a bunch of computations
   * from T => Future[U].
   *
   * At a high level, the computation performed by the multiGet is the following:
   *
   * - Look up the set of requested keys in the offlineStore. The offlineStore holds K -> (BatchID, V).
   * - For each key, use the returned BatchID and the current BatchID (calculated by the batcher)
   *   to generate a sequence of BatchIDs that the onlineStore is holding. The onlineStore holds
   *   (K, BatchID) -> V, so a join between this BatchID sequence and the K provides a keyset to
   *   use for a multiGet to the onlineStore.
   * - Perform this multiGet to the online store.
   * - PIVOT the BatchIDs out of the online store's key into a sequence in the value -- then
   *   a monoid merge with the offline store will append the offline value onto the beginning
   *   of the sequence of (BatchID, V).
   * - Finally, reduce this list by monoid-merging together all (BatchID, V) pairs. If any
   *   BatchID is missing from the sequence (if there are any holes, for example), that
   *   particular merged value's computation in the result will be a Future.exception
   *   vs a defined future.
   * - Drop the final BatchID off of all successfully aggregated values (since this BatchID
   *   will be the current batch in all successful cases).
   */
  override def multiGet(ks: Set[K]): Future[Map[K, Future[Option[V]]]] =
    for (offlineResult: Map[K, Future[Option[(BatchID, V)]]] <- offlineStore.multiGet(ks);
         liftedOffline = offlineResult mapValues { _.map { Seq(_) } };
         onlineKeys <- generateOnlineKeys(ks.toSeq, batcher.currentBatch)(offlineResult);
         onlineResult: Map[(K, BatchID), Future[Option[V]]]  <- onlineStore.multiGet(onlineKeys);
         liftedOnline: Map[K, Future[Seq[Option[(BatchID, V)]]]] = pivotBatches(onlineResult))
    yield dropBatches(mergeResults(liftedOffline, liftedOnline))
}
