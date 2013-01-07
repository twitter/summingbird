package com.twitter.summingbird.sink

import com.twitter.algebird.Monoid
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.util.Future

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Online sink that throws errors for all online-only methods. Meant to fill
 * in the holes in a CompoundSink for an offline-only summingbird job.
 */

class EmptyOnlineSink[Time,Key,Value](override val batcher: Batcher[Time],
                                      override val monoid: Monoid[Value])
extends OnlineSink[Time,Key,Value] {
  override def batchesToKeep = BatchesToKeep(1)
  override def getOnline(k: (Key,BatchID)) = Future.None
  override def multiGetOnline(ks: Set[(Key,BatchID)]) = Future.value(Map.empty[(Key,BatchID),Value])
  override def increment(pair: ((Key,BatchID),Value)) = {
    throw new UnsupportedOperationException("Offline sinks don't support random writes.")
  }
  override def decoderParallelism =
    throw new UnsupportedOperationException("Offline sinks don't support \"boltParallelism\".")
  override def rpcParallelism =
    throw new UnsupportedOperationException("Offline sinks don't support \"boltParallelism\".")
  override def sinkParallelism =
    throw new UnsupportedOperationException("Offline sinks don't support \"boltParallelism\".")
}
