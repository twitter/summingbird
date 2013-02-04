package com.twitter.summingbird.scalding.store

import cascading.flow.FlowDef

import com.twitter.scalding.{ Dsl, Mode, TDsl, TypedPipe, IterableSource }
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.util.Future

import java.io.Serializable

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object BatchReadableStore {
  import Dsl._
  import TDsl._

  def empty[K,V](batchID: BatchID): BatchReadableStore[K,V] =
    new BatchReadableStore[K,V] {
      override def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode): (BatchID, TypedPipe[(K,V)]) =
        (batchID, mappableToTypedPipe(IterableSource(Seq[(K,V)]())))
    }
}

trait BatchReadableStore[K, V] extends Serializable {
  // Return the previous upper bound batchId and the pipe covering this range
  // NONE of the items from the returned batchID should be present in the TypedPipe
  def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode)
  : (BatchID, TypedPipe[(K,V)])
}

trait BatchStore[K,V] extends BatchReadableStore[K,V] with Serializable {
  def write(env: ScaldingEnv, p: TypedPipe[(K,V)])
  (implicit fd: FlowDef, mode: Mode): Unit
  def commit(batchId: BatchID, env: ScaldingEnv): Unit
}

class EmptyBatchStore[K, V] extends BatchStore[K, V] {
  val empty = BatchReadableStore.empty[K,V](BatchID(0))
  override def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode) = empty.readLatest(env)
  def write(env: ScaldingEnv, p: TypedPipe[(K,V)])(implicit fd: FlowDef, mode: Mode) { }
  def commit(batchId: BatchID, env: ScaldingEnv) { }
}
