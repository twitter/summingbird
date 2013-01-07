package com.twitter.summingbird.sink

import cascading.flow.FlowDef
import com.twitter.algebird.Monoid
import com.twitter.bijection.Bijection
import com.twitter.scalding.{ TypedPipe, Mode, Dsl }
import com.twitter.scalding.commons.source.VersionedKeyValSource
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.scalding.ScaldingEnv

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Scalding implementation of the batch  read and write components
 * of a sink that uses the VersionedKeyValSource from scalding-commons.
 */

abstract class ScaldingVersionedSink[Time,Key,Value](rootPath: String)
(implicit tord: Ordering[Time],
 override val batcher: Batcher[Time],
 override val monoid: Monoid[Value],
 kCodec: Bijection[Key,Array[Byte]],
 vCodec: Bijection[(BatchID,Value),Array[Byte]])
extends OfflineSink[Time,Key,Value] {
  import Dsl._

  // ## Batch-write Components

  def prevUpperBound(env: ScaldingEnv): BatchID =
    HDFSMetadata.get[String](env.config, rootPath)
      .map { BatchID(_) }
      .orElse(env.startBatch(batcher))
      .get

  def readLatest(env: ScaldingEnv)
  (implicit fd: FlowDef, mode: Mode): (BatchID, TypedPipe[(Key,Value)]) = {
    val batchID = prevUpperBound(env)

    val pipe = TypedPipe
      .from[(Key,(BatchID,Value))](VersionedKeyValSource[Key,(BatchID,Value)](rootPath).read, (0,1))
      //Discard the batchID which is only needed for a join with realtime:
      .map { kvb => (kvb._1, kvb._2._2) }

    (batchID, pipe)
  }

  def write(env: ScaldingEnv, p: TypedPipe[(Key,Value)])
  (implicit fd: FlowDef, mode: Mode) {

    val batchID = prevUpperBound(env) + env.batches

    p.map { kv => (kv._1, (batchID, kv._2)) }
      .write((0,1), VersionedKeyValSource[Key,(BatchID,Value)](rootPath))
  }

  def commit(start: BatchID, env: ScaldingEnv) {
    HDFSMetadata.put(env.config, rootPath, Some((start + env.batches).toString))
  }
}
