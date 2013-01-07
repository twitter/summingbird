package com.twitter.summingbird.sink

import com.twitter.util.Future
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.store._
import com.twitter.algebird.Monoid
import com.twitter.scalding.TypedPipe
import com.twitter.scalding.Mode
import cascading.flow.FlowDef

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object MemorySink {
  def apply[StoreType <: Store[StoreType,(Key,BatchID),Value],Time,Key,Value]
  (store: StoreType = new MapStore[(Key,BatchID),Value])
  (implicit batchesToKeep: BatchesToKeep,
   batcher: Batcher[Time],
   monoid: Monoid[Value],
   decoderParallelism: DecoderParallelism,
   rpcParallelism: RpcParallelism,
   sinkParallelism: SinkParallelism,
   timeord: Ordering[Time]) = new MemorySink[StoreType,Time,Key,Value](Future.value(store))
}

class MemorySink[StoreType <: Store[StoreType,(Key,BatchID),Value],Time,Key,Value](var storeFuture: Future[StoreType])
(implicit override val batchesToKeep: BatchesToKeep,
 override val batcher: Batcher[Time],
 override val monoid: Monoid[Value],
 override val decoderParallelism: DecoderParallelism,
 override val rpcParallelism: RpcParallelism,
 override val sinkParallelism: SinkParallelism,
 timeord: Ordering[Time])
extends OnlineSink[Time,Key,Value] {
  override def getOnline(k: (Key,BatchID)) = storeFuture flatMap { _.get(k) }
  override def multiGetOnline(ks: Set[(Key,BatchID)]) = storeFuture flatMap { _.multiGet(ks) }

  override def increment(pair: ((Key,BatchID),Value)) = {
    val (k,v) = pair
    val newStoreFuture =
      storeFuture flatMap { store =>
        store.update(k) { oldV =>
          monoid.nonZeroOption(oldV.map { monoid.plus(_,v) } getOrElse v)
        }
      }

    // Assignment happens after future creation to ensure that the var
    // assignment happens AFTER the read.
    storeFuture = newStoreFuture

    // flatMap against newStoreFuture to ensure a single read
    // of the stagingStoreFuture var per call to increment.
    newStoreFuture flatMap { _ => Future.Unit }
  }
}
