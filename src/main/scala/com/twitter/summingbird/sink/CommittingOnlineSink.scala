package com.twitter.summingbird.sink

import com.twitter.algebird.Monoid
import com.twitter.util.Future
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.store.{ MapStore, ConcurrentMutableStore, Store }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * The CommittingOnlineSink wraps a ConcurrentMutableStore in a way that allows for batched
 * updates. The CommittingOnlineSink uses its Monoid[Value] to pre-aggregate key-value pairs
 * into an internal MapStore[Key,Value], only committing out to the wrapped mutable store
 * when the size of the internal store grows beyond a specified size (maxKeys).
 *
 * On read, the ConcurrentMutableStore will read a value out of both the backing mutable store
 * and the internal store and merge them together with the Monoid[Value]. On system failure,
 * this store will fail to commit at most (maxKeys - 1) total key-value pairs.
 */

object CommittingOnlineSink {
  def apply[StoreType <: ConcurrentMutableStore[StoreType,(Key,BatchID),Value],Time,Key,Value]
  (maxKeys: Int, committedStore: StoreType)
  (implicit batchesToKeep: BatchesToKeep, batcher: Batcher[Time], monoid: Monoid[Value],
   decoderParallelism: DecoderParallelism, rpcParallelism: RpcParallelism,
   sinkParallelism: SinkParallelism, timeord: Ordering[Time]) =
     new CommittingOnlineSink[StoreType,Time,Key,Value](maxKeys, committedStore)
}

@deprecated("""Use com.twitter.summingbird.sink.BufferingSink
            (unless you're accessing the sink through Storm's DRPC mechanism and really
            care about accessing the small uncommitted portion of data still in staging).""",
            "https://reviewboard.twitter.biz/r/104192/")
class CommittingOnlineSink[StoreType <: ConcurrentMutableStore[StoreType,(Key,BatchID),Value],Time,Key,Value]
(maxKeys: Int, committedStore: StoreType)
(implicit override val batchesToKeep: BatchesToKeep,
 override val batcher: Batcher[Time],
 override val monoid: Monoid[Value],
 override val decoderParallelism: DecoderParallelism,
 override val rpcParallelism: RpcParallelism,
 override val sinkParallelism: SinkParallelism,
 timeord: Ordering[Time])
extends OnlineSink[Time,Key,Value] {

  var stagingStoreFuture: Future[MapStore[(Key,BatchID),Value]] =
    Future.value(new MapStore[(Key,BatchID),Value])

  override def getOnline(batchedKey: (Key,BatchID)): Future[Option[Value]] =
    stagingStoreFuture flatMap { stagingStore =>
      stagingStore.get(batchedKey)
        .join(committedStore.get(batchedKey))
        .map { pair =>
          pair match {
            case (Some(v1), Some(v2)) => Some(monoid.plus(v1,v2));
            case (None, a@_) => a;
            case (a@_, None) => a;
          }
        }
    }

  // incStore updates the supplied key in the supplied
  // ConcurrentMutableStore with a function from Option[V] =>
  // Option[V].
  //
  // If the key is already present in the mutable store, this function
  // monoid-adds the new value "v" into the store. If the key isn't
  // present, the function returns an option on the new value "v". If
  // either of these cases returns the monoid zero, the wrapping call
  // to monoid.nonZeroOption will return None, removing the key from
  // the mutable store.

  protected def incStore[S <: Store[S,(Key,BatchID),Value]](s: S, k: (Key,BatchID), v: Value): Future[S] = {
    s.update(k) { oldV =>
      monoid.nonZeroOption(oldV.map { monoid.plus(_,v) } getOrElse v)
    }
  }

  // the stagingStore is a MapStore, so we can access the backing
  // immutable scala map. "par" converts this backing map into a
  // parallel collection, allowing a parallelized sinking of all
  // key-value pairs into the wrapped ConcurrentMutableStore.
  //
  // See this blog post for more info on Scala's parallel collections:
  // http://beust.com/weblog/2011/08/15/scalas-parallel-collections/

  def commit(mapStore: MapStore[(Key,BatchID),Value]): Future[Unit] = {
    // build up an iterable of futures, each of which is responsible
    // for committing a single key-value pair into the committedStore:
    val commitFutures = mapStore.backingStore.map { case (k,v) =>
      incStore(committedStore, k, v)
    }
    Future.join(commitFutures.toSeq)
  }


  override def increment(pair: ((Key,BatchID),Value)) = {
    val (kPair,v) = pair
    val newStagingStoreFuture =
      stagingStoreFuture
        .flatMap { incStore(_, kPair, v) }
        .flatMap { stagingStore =>
          if (stagingStore.size >= maxKeys)
            commit(stagingStore) map { _ => new MapStore[(Key,BatchID),Value] }
          else
            Future(stagingStore)
        }
    // Assignment happens after future creation to ensure that the var
    // assignment happens AFTER the read.
    stagingStoreFuture = newStagingStoreFuture

    // flatMap against newStagingStoreFuture to ensure a single read
    // of the stagingStoreFuture var per call to increment.
    newStagingStoreFuture flatMap { _ => Future.Unit }
  }
}
