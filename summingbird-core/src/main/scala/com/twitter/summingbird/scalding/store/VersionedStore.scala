package com.twitter.summingbird.scalding.store

import cascading.flow.FlowDef
import com.twitter.bijection.Bijection
import com.twitter.scalding.{ TypedPipe, Mode, Dsl, TDsl }
import com.twitter.scalding.commons.source.VersionedKeyValSource
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.ScaldingEnv

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Scalding implementation of the batch read and write components
 * of a store that uses the VersionedKeyValSource from scalding-commons.
 */

object VersionedStore {
  def apply[Key, Value](rootPath: String)
  (implicit bijection: Bijection[(Key, Value), (Array[Byte], Array[Byte])]) =
    new VersionedStore[Key, Value](VersionedKeyValSource[Key, Value](rootPath))

  def apply[Key, Value](source: => VersionedKeyValSource[Key, Value]) =
    new VersionedStore[Key, Value](source)
}

// TODO: it looks like when we get the mappable/directory this happens
// at a different time (not atomically) with getting the
// meta-data. This seems like something we need to fix: atomically get
// meta-data and open the Mappable.
// The source parameter is pass-by-name to avoid needing the hadoop
// Configuration object when running the storm job.
class VersionedStore[Key, Value](source: => VersionedKeyValSource[Key, Value]) extends BatchStore[Key, Value] {
  import Dsl._
  import TDsl._

  // ## Batch-write Components

  // TODO: the "orElse" logic already exists in the
  // BatchAggregatorJob. Remove it from here.
  def prevUpperBound(env: ScaldingEnv): BatchID =
    HDFSMetadata.get[String](env.config, source.path)
      .map { BatchID(_) }
      .orElse(env.startBatch(env.builder.batcher))
      .get

  // TODO: Note that source is EMPTY if the BatchID doesn't
  // exist. We should really be returning an option of the
  // BatchID, TypedPipe pair.
  def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode): (BatchID, TypedPipe[(Key,Value)]) =
    (prevUpperBound(env), source)

  def write(env: ScaldingEnv, p: TypedPipe[(Key,Value)])
  (implicit fd: FlowDef, mode: Mode) {
    p.write((0,1), source)
  }

  def commit(batchID: BatchID, env: ScaldingEnv) {
    HDFSMetadata.put(env.config, source.path, Some(batchID.toString))
  }
}
