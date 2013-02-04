package com.twitter.summingbird.scalding.store

import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.algebird.Monoid
import com.twitter.scalding.{Dsl, Mode, TypedPipe, TDsl, Tsv}
import cascading.flow.FlowDef

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object TsvStore {
  def apply[Key, Value](path: String) = new TsvStore(path)
}

class TsvStore[Key, Value](path: String) extends BatchStore[Key, Value] {
  import Dsl._
  import TDsl._

  // TODO write metadata about the BatchID in alongside the path.
  override def readLatest(env: ScaldingEnv)
  (implicit fd: FlowDef, mode: Mode): (BatchID, TypedPipe[(Key,Value)]) =
    (BatchID(0), Tsv(path).read.toTypedPipe[(Key,Value)]((0,1)))

  override def write(env : ScaldingEnv, p : TypedPipe[(Key,Value)])
    (implicit fd : FlowDef, mode : Mode) {
    import Dsl._
    import TDsl._
    p.write(('key, 'value), Tsv(path))
  }

  override def commit(batchid : BatchID, env : ScaldingEnv) { }
}
