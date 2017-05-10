/*
Copyright 2014 Twitter, Inc.

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

import com.twitter.algebird.{ MapMonoid, Monoid, Semigroup }
import com.twitter.algebird.util.UtilAlgebras._
import com.twitter.storehaus.{ FutureCollector, FutureOps, ReadableStore }
import com.twitter.storehaus.algebra.Mergeable
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.util.Future

import scala.collection.breakOut

object ClientMergeable {
  def apply[K, V](
    offlineStore: ReadableStore[K, (BatchID, V)],
    onlineStore: ReadableStore[(K, BatchID), V] with Mergeable[(K, BatchID), V],
    batchesToKeep: Int)(implicit batcher: Batcher, monoid: Semigroup[V]): ClientMergeable[K, V] =
    new ClientMergeable[K, V](offlineStore, onlineStore,
      batcher, batchesToKeep, ClientStore.defaultOnlineKeyFilter[K], FutureCollector.bestEffort)
}

/**
 * This is like a ClientStore except that it can perform
 * a merge into the online store and return the total sum.
 */
class ClientMergeable[K, V: Semigroup](
    offlineStore: ReadableStore[K, (BatchID, V)],
    onlineStore: ReadableStore[(K, BatchID), V] with Mergeable[(K, BatchID), V],
    batcher: Batcher,
    batchesToKeep: Int,
    onlineKeyFilter: K => Boolean,
    collector: FutureCollector) extends Mergeable[(K, BatchID), V] {

  def readable: ClientStore[K, V] =
    new ClientStore(offlineStore, onlineStore, batcher, batchesToKeep, onlineKeyFilter, collector)

  import MergeOperations.FOpt

  def semigroup = implicitly[Semigroup[V]]

  private def fsg: Semigroup[FOpt[V]] = new Semigroup[FOpt[V]] {
    def plus(left: FOpt[V], right: FOpt[V]): FOpt[V] =
      left.join(right).map { case (l, v) => Monoid.plus(l, v) }
  }
  // This should not be needed, but somehow this FOpt breaks the implicit resolution
  private val mm = new MapMonoid[K, FOpt[V]]()(fsg)

  override def merge(kbv: ((K, BatchID), V)): FOpt[V] = {
    val ((key, batch), delta) = kbv
    val existing: FOpt[V] = readable.multiGetBatch[K](batch.prev, Set(key))(key)
    // Now we merge into the current store:
    val preMerge: FOpt[V] = onlineStore.merge(kbv)
    fsg.plus(existing, preMerge)
  }
  override def multiMerge[K1 <: (K, BatchID)](ks: Map[K1, V]): Map[K1, FOpt[V]] = {
    /*
     * We start by finding the min BatchId for each K, and merge those ((K, BatchID), V) into the
     * store first as they make up the basis of any (K, BatchID') where BatchID' > BatchID.
     *
     * This could be optimized more, but since
     * mergeing two different batches for the same key is presumably rare,
     * it is probably not worth it.
     */
    val batchForKey: Map[K1, V] = ks.groupBy { case ((k, batchId), v) => k }
      .iterator
      .map { case (k, kvs) => kvs.minBy { case ((_, batchId), _) => batchId } }
      .toMap

    val nextCall = ks -- batchForKey.keys

    val firstRes = multiMergeUnique(batchForKey)
    if (nextCall.nonEmpty) {
      // Wait for these values to make it in, then call:
      val previousIsDone: Future[Unit] =
        Future.collect(firstRes.iterator.map(_._2).toIndexedSeq).unit

      val fmap = previousIsDone.map { _ =>
        multiMerge(nextCall)
      }
      firstRes ++ FutureOps.liftFutureValues(nextCall.keySet, fmap)
    } else firstRes
  }

  private def multiMergeUnique[K1 <: (K, BatchID)](ks: Map[K1, V]): Map[K1, FOpt[V]] = {
    // Here we assume each K appears only once, because the previous call ensures it
    val result = ks.groupBy { case ((_, batchId), _) => batchId }
      .iterator
      .map {
        case (batch, kvs) =>
          val batchKeys: Set[K] = kvs.map { case ((k, _), _) => k }(breakOut)
          val existing: Map[K, FOpt[V]] = readable.multiGetBatch[K](batch.prev, batchKeys)
          // Now we merge into the current store:
          val preMerge: Map[K, FOpt[V]] = onlineStore.multiMerge(kvs)
            .map { case ((k, _), v) => (k, v) }(breakOut)

          (batch, mm.plus(existing, preMerge))
      }
      .flatMap {
        case (b, kvs) =>
          kvs.iterator.map { case (k, v) => ((k, b), v) }
      }
      .toMap
    // Since the type is a subclass, we need to jump through this hoop:
    ks.iterator.map { case (k1, _) => (k1, result(k1)) }.toMap
  }
}
