package com.twitter.summingbird.sink

import cascading.flow.FlowDef
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ Mode, TypedPipe }
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.util.Future

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Offline sink that throws errors for all offline-only methods. Meant to fill
 * in the holes in a CompoundSink for an online-only summingbird job.
 */

class EmptyOfflineSink[Time,Key,Value](override val batcher: Batcher[Time], override val monoid: Monoid[Value])
extends OfflineSink[Time,Key,Value] {
  override def getOffline(k: Key) = Future.None
  override def multiGetOffline(ks: Set[Key]) = Future.value(Map.empty[Key,(BatchID,Value)])
  override def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode) =
    throw new UnsupportedOperationException("Online sinks don't support \"readLatest\".")

  override def write(env: ScaldingEnv, p: TypedPipe[(Key,Value)])(implicit fd: FlowDef, mode: Mode) {
    throw new UnsupportedOperationException("Online sinks don't support bulk writes.")
  }

  override def commit(bid : BatchID, env: ScaldingEnv) {
    throw new UnsupportedOperationException("Online sinks don't support \"commit\".")
  }
}
