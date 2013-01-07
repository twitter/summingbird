package com.twitter.summingbird.sink

import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.algebird.Monoid
import com.twitter.scalding.{Dsl, Mode, TypedPipe, TDsl, Tsv}
import cascading.flow.FlowDef

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * ScaldingTSVSink is an OfflineSink with no support for random reads,
 * just bulk writes and reads. This sink will probably only be used
 * for testing.
 */

abstract class ScaldingTsvSink[Time,Key,Value](path: String)
(implicit override val monoid: Monoid[Value],
 override val batcher: Batcher[Time],
 timeord: Ordering[Time])
extends OfflineSink[Time,Key,Value] {
  override def readLatest(env: ScaldingEnv)
  (implicit fd: FlowDef, mode: Mode): (BatchID, TypedPipe[(Key,Value)]) = {
    import Dsl._
    import TDsl._
    // TODO read some meta-data about the batchID.
    (BatchID(0), Tsv(path).read.toTypedPipe[(Key,Value)]((0,1)))
  }

  override def write(env : ScaldingEnv, p : TypedPipe[(Key,Value)])
    (implicit fd : FlowDef, mode : Mode) {
    import Dsl._
    import TDsl._
    p.write(('key, 'value), Tsv(path))
  }

  override def commit(batchid : BatchID, env : ScaldingEnv) { }
}
