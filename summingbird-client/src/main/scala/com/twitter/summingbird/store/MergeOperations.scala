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
import com.twitter.algebird.util.UtilAlgebras._
import com.twitter.bijection.Pivot
import com.twitter.storehaus.FutureCollector
import com.twitter.summingbird.batch.BatchID
import com.twitter.util.{ Future, Return, Try }

object MergeOperations {
  type FOpt[T] = Future[Option[T]]

  // TODO (https://github.com/twitter/summingbird/issues/71): the
  // check here on sequential batch IDs was wrong. we should be
  // checking that the batch layer is not more than batchesToKeep
  // behind. the manner of doing that on a per-key basis will change
  // in storehaus 0.3, so add in that check during the upgrade.
  def sortedSum[V: Semigroup](opts: Seq[Option[(BatchID, V)]]): Option[(BatchID, V)] =
    Semigroup.sumOption(opts.flatten.sortBy(_._1))

  /**
   * Pivot to pivot (K, BatchID) pairs into K -> (BatchID, V).
   */
  def pivot[K] = Pivot.of[(K, BatchID), K, BatchID]

  def collect[T, U](seq: Seq[(T, Future[U])])(implicit collect: FutureCollector[(T, U)]): Future[Seq[(T, U)]] =
    collect {
      seq.map { case (t, futureU) => futureU.map(t -> _) }
    }

  def mergeResults[K, V: Semigroup](
    m1: Map[K, Future[Seq[Option[(BatchID, V)]]]],
    m2: Map[K, Future[Seq[Option[(BatchID, V)]]]]): Map[K, Future[Option[(BatchID, V)]]] =
    Semigroup.plus(m1, m2).map {
      case (k, v) =>
        k -> v.map(sortedSum(_))
    }

  def dropBatches[K, V](m: Map[K, Future[Option[(BatchID, V)]]]): Map[K, Future[Option[V]]] =
    m.map { case (k, v) => k -> v.map(_.map(_._2)) }

  /**
   * Pivots each BatchID out of the key and into the Value's future.
   */
  def pivotBatches[K, V](m: Map[(K, BatchID), FOpt[V]]): Map[K, Future[Seq[Option[(BatchID, V)]]]] =
    pivot.withValue(m).map {
      case (k, it) =>
        k -> collect(it.toSeq).map { _.map { case (batchID, optV) => optV.map(batchID -> _) } }
    }

  def decrementOfflineBatch[K, V](m: Map[K, FOpt[(BatchID, V)]]): Map[K, Future[Seq[Option[(BatchID, V)]]]] =
    m.map {
      case (k, futureOptV) =>
        k -> futureOptV.map { optV =>
          Seq(optV.map { case (batchID, v) => (batchID.prev, v) })
        }
    }

  /**
   * Selects the most recent BatchID between the offlineStore and the BatchID calculated
   * with batchesToKeep. The more recent BatchID is used as the begining of the
   * range used to query the onlineStore.
   */
  def expand(offlineReturn: Option[BatchID], nowBatch: BatchID, batchesToKeep: Int): Iterable[BatchID] = {
    val initBatch = Semigroup.plus(
      Some(nowBatch - (batchesToKeep - 1)), offlineReturn
    ).get // This will never throw, as this option can never be None
    // (because we included an explicit "Some" inside)
    BatchID.range(initBatch, nowBatch)
  }

  def generateOnlineKeys[K](ks: Seq[K], nowBatch: BatchID, batchesToKeep: Int)(lookup: K => FOpt[BatchID])(implicit collect: FutureCollector[(K, Iterable[BatchID])]): Future[Set[(K, BatchID)]] =
    for {
      collected <- collect(
        ks.map { k => lookup(k).map { k -> expand(_, nowBatch, batchesToKeep) } }
      )
    } yield pivot.invert(collected.toMap).toSet
}
