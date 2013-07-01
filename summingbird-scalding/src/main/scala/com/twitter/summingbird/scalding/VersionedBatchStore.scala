/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.summingbird.scalding

import com.twitter.summingbird.scalding.store.HDFSMetadata
import cascading.flow.FlowDef
import com.twitter.bijection.Injection
import cascading.flow.FlowDef
import com.twitter.scalding.{Dsl, Mode, TDsl, TypedPipe, Hdfs => HdfsMode, TupleSetter}
import com.twitter.scalding.commons.source.VersionedKeyValSource
import com.twitter.summingbird.monad.Reader
import com.twitter.summingbird.batch.{BatchID, Batcher}
import scala.util.control.Exception.allCatch

/**
 * Scalding implementation of the batch read and write components
 * of a store that uses the VersionedKValSource from scalding-commons.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object VersionedBatchStore {

}

// TODO: it looks like when we get the mappable/directory this happens
// at a different time (not atomically) with getting the
// meta-data. This seems like something we need to fix: atomically get
// meta-data and open the Mappable.
// The source parameter is pass-by-name to avoid needing the hadoop
// Configuration object when running the storm job.
class VersionedBatchStore[K, V](rootPath: String, versionsToKeep: Int, override val batcher: Batcher)(
    implicit injection: Injection[(K, V), (Array[Byte], Array[Byte])], override val ordering: Ordering[K])
  extends BatchedScaldingStore[K, V] {

  /** Get the most recent last batch and the ID (strictly less than the input ID)
   * The "Last" is the stream with only the newest value for each key, within the batch
   * combining the last from batchID and the deltas from batchID.next you get the stream
   * for batchID.next
   */
  override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    mode match {
      case hdfs: HdfsMode =>
        lastBatch(exclusiveUB, hdfs)
          .map { Right(_) }
          .getOrElse {
            Left(List("No last batch available < %s for VersionedBatchStore(%s)".format(exclusiveUB, rootPath)))
          }
      case _ => Left(List("Mode: %s not supported for VersionedBatchStore(%s)".format(mode, rootPath)))
    }
  }

  /** Instances may choose to write out the last or just compute it from the stream */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    import Dsl._
    implicit val ts: TupleSetter[(K,V)] = Dsl.Tup2Setter
    lastVals.toPipe(Dsl.intFields(List(0,1)))
      .write(VersionedKeyValSource(rootPath,
          sourceVersion=None,
          sinkVersion=Some(batchToVersion(batchID)),
          maxFailures=0,
          versionsToKeep=versionsToKeep))
  }

  // Use the exclusive upper bound as the time
  def batchToVersion(b: BatchID): Long = batcher.earliestTimeOf(b.next).getTime
  def versionToBatchID(ver: Long): BatchID = batcher.batchOf(new java.util.Date(ver)).prev

  protected def lastBatch(exclusiveUB: BatchID, mode: HdfsMode): Option[(BatchID, FlowProducer[TypedPipe[(K,V)]])] = {
    val meta = HDFSMetadata(mode.conf, rootPath)
    // TODO remove this when all sources have run for a while with the new version format
    def versionToBatchIDCompat(ver: Long): BatchID = {
      //First try to read the old-style metadata:
      meta(ver)
        .get[String]
        .flatMap { str => allCatch.opt(BatchID(str)) }
        .getOrElse(versionToBatchID(ver))
    }
    meta
      .versions.map { ver => (versionToBatchIDCompat(ver), readVersion(ver)) }
      .filter { _._1 < exclusiveUB }
      .reduceOption { (a, b) => if (a._1 > b._1) a else b }
  }

  protected def readVersion(v: Long): FlowProducer[TypedPipe[(K,V)]] = Reader { (flowMode: (FlowDef, Mode)) =>
    val mappable = VersionedKeyValSource(rootPath, sourceVersion=Some(v))
    TypedPipe.from(mappable)(flowMode._1, flowMode._2, mappable.converter)
  }
}
