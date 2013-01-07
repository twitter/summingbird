package com.twitter.summingbird.sink

import com.twitter.algebird.{ SummingQueue, Monoid }
import com.twitter.util.Future
import com.twitter.summingbird.store.ConcurrentMutableStore
import com.twitter.summingbird.util.CacheSize
import com.twitter.summingbird.batch.{ Batcher, BatchID }

/**
 *  @author Ashu Singhal
 */

object BufferingSink {
  def apply[StoreType <: ConcurrentMutableStore[StoreType,(Key,BatchID),Value],Time,Key,Value]
  (cacheSize: CacheSize, committedStore: StoreType)
  (implicit batchesToKeep: BatchesToKeep, batcher: Batcher[Time], monoid: Monoid[Value],
   decoderParallelism: DecoderParallelism, rpcParallelism: RpcParallelism,
   sinkParallelism: SinkParallelism) =
     new BufferingSink[StoreType,Time,Key,Value](cacheSize, committedStore)
}

// TODO: Merge this into CommittingOnlineStore. Convert to use
// CacheSize and validate that the CacheSize is positive.
class BufferingSink[StoreType <: ConcurrentMutableStore[StoreType,(Key,BatchID),Value],Time,Key,Value]
(cacheSize: CacheSize, committedStore: StoreType)
(implicit override val batchesToKeep: BatchesToKeep,
 override val batcher: Batcher[Time],
 override val monoid: Monoid[Value],
 override val decoderParallelism: DecoderParallelism,
 override val rpcParallelism: RpcParallelism,
 override val sinkParallelism: SinkParallelism) extends OnlineSink[Time, Key, Value] {

  override def getOnline(pair: (Key,BatchID)) = committedStore.get(pair)
  override def multiGetOnline(pairs: Set[(Key,BatchID)]) =
    committedStore.multiGet(pairs)

  protected def incStore(item: ((Key,BatchID),Value)) = {
    val (k,v) = item
    committedStore.update(k) { oldV =>
      monoid.nonZeroOption(oldV.map { monoid.plus(_,v) } getOrElse v)
    }.unit
  }

  protected def multiIncrement(items: Map[(Key,BatchID),Value]) = {
    val futures = items map { incStore(_) }
    Future.collect(futures.toSeq).unit
  }

  lazy val buffer = SummingQueue[Map[(Key,BatchID),Value]](cacheSize.size.getOrElse(1))

  override def increment(pair: ((Key,BatchID),Value)) =
    buffer(Map(pair))
      .map { multiIncrement(_) }
      .getOrElse(Future.Unit)
}
