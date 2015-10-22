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

package com.twitter.summingbird.scalding.store

import cascading.flow.FlowDef
import com.twitter.algebird.monad.Reader
import com.twitter.bijection.Injection
import com.twitter.scalding.commons.source.VersionedKeyValSource
import com.twitter.scalding.{ Mode, TypedPipe, Hdfs => HdfsMode, TupleSetter }
import com.twitter.summingbird.batch.store.HDFSMetadata
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp, OrderedFromOrderingExt }
import com.twitter.summingbird.scalding._
import com.twitter.summingbird.scalding.batch.BatchedStore
import com.twitter.summingbird.scalding.{ Try, FlowProducer, Scalding }
import org.slf4j.LoggerFactory
import scala.util.{ Try => ScalaTry }

/**
 * Scalding implementation of the batch read and write components of a
 * store that uses the VersionedKeyValSource from scalding-commons.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object VersionedBatchStore {
  def apply[K, V, K2, V2](rootPath: String, versionsToKeep: Int)(pack: (BatchID, (K, V)) => (K2, V2))(unpack: ((K2, V2)) => (K, V))(
    implicit batcher: Batcher,
    injection: Injection[(K2, V2), (Array[Byte], Array[Byte])],
    ordering: Ordering[K]): VersionedBatchStore[K, V, K2, V2] =
    new VersionedBatchStore(rootPath, versionsToKeep, batcher)(pack)(unpack)
}

/**
 * Allows subclasses to share the means of reading version numbers but
 * plug in methods to actually read or write the data.
 */
abstract class VersionedBatchStoreBase[K, V](val rootPath: String) extends BatchedStore[K, V] {
  import OrderedFromOrderingExt._
  /**
   * Returns a snapshot of the store's (K, V) pairs aggregated up to
   * (but not including!) the time covered by the supplied batchID.
   *
   * Aggregating the readLast for a particular batchID with the
   * stream stored for the same batchID will return the aggregate up
   * to (but not including) batchID.next. Streams deal with inclusive
   * upper bound.
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

  /**
   * These functions convert back and forth between a specific
   * BatchID and the earliest time of the BatchID just after it.
   *
   * The version numbers are the exclusive upper-bound of time
   * covered by this store, while the batchIDs are the inclusive
   * upper bound. Put another way, all events that occured before the
   * version are included in this store.
   */
  def batchIDToVersion(b: BatchID): Long = batcher.earliestTimeOf(b.next).milliSinceEpoch
  def versionToBatchID(ver: Long): BatchID = batcher.batchOf(Timestamp(ver)).prev

  protected def lastBatch(exclusiveUB: BatchID, mode: HdfsMode): Option[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    val meta = HDFSMetadata(mode.conf, rootPath)
    /*
     * The deprecated Summingbird builder API coordinated versioning
     * through a _summingbird.json dropped into each version of this
     * store's VersionedStore.
     *
     * The new API (as of 0.1.0) coordinates this state via the actual
     * version numbers within the VersionedStore. This function
     * resolves the BatchID out of a version by first checking the
     * metadata inside of the version; if the metadata exists, it
     * takes preference over the version number (which was garbage,
     * just wall clock time, in the deprecated API). If the metadata
     * does NOT exist we know that the version is meaningful and
     * convert it to a batchID.
     *
     * TODO (https://github.com/twitter/summingbird/issues/95): remove
     * this when all internal Twitter jobs have run for a while with
     * the new version format.
     */
    def versionToBatchIDCompat(ver: Long): BatchID = {
      /**
       * Old style writes the UPPER BOUND batchID, so all times
       * are in a batch LESS than the value in the file.
       */
      meta(ver)
        .get[String]
        .flatMap { str => ScalaTry(BatchID(str).prev) }
        .map { oldbatch =>
          val newBatch = versionToBatchID(ver)
          if (newBatch > oldbatch) {
            println("## WARNING ##")
            println("in BatchStore(%s)".format(rootPath))
            println("Old-style version number is ahead of what the new-style would be.")
            println("Until batchID: %s (%s) you will see this warning"
              .format(newBatch, batcher.earliestTimeOf(newBatch)))
            println("##---------##")
          }
          oldbatch
        }
        .getOrElse(versionToBatchID(ver))
    }
    meta
      .versions.map { ver => (versionToBatchIDCompat(ver), readVersion(ver)) }
      .filter { _._1 < exclusiveUB }
      .reduceOption { (a, b) => if (a._1 > b._1) a else b }
  }

  protected def readVersion(v: Long): FlowProducer[TypedPipe[(K, V)]]
}

/*
 * TODO (https://github.com/twitter/summingbird/issues/94): it looks
 * like when we get the mappable/directory this happens at a different
 * time (not atomically) with getting the meta-data. This seems like
 * something we need to fix: atomically get meta-data and open the
 * Mappable.  The source parameter is pass-by-name to avoid needing
 * the hadoop Configuration object when running the storm job.
 */
class VersionedBatchStore[K, V, K2, V2](rootPath: String, versionsToKeep: Int, override val batcher: Batcher)(pack: (BatchID, (K, V)) => (K2, V2))(unpack: ((K2, V2)) => (K, V))(
  implicit @transient injection: Injection[(K2, V2), (Array[Byte], Array[Byte])], override val ordering: Ordering[K])
    extends VersionedBatchStoreBase[K, V](rootPath) {
  @transient private val logger = LoggerFactory.getLogger(classOf[VersionedBatchStore[_, _, _, _]])

  override def toString: String = s"${this.getClass.getSimpleName}(rootPath=$rootPath, versionesToKeep=$versionsToKeep, batcher=$batcher)"

  /**
   * Make sure not to keep more than versionsToKeep when we write out.
   * If this is out of sync with VersionedKeyValSource we can have issues
   */
  override def select(b: List[BatchID]): List[BatchID] = b.takeRight(versionsToKeep)

  /**
   * writeLast receives an INCLUSIVE upper bound on batchID and a
   * pipe of all key-value pairs aggregated up to (and including)
   * that batchID. (Yes, this is confusing, since a number of other
   * methods talk about the EXCLUSIVE upper bound.)
   *
   * This implementation of writeLast sinks all key-value pairs out
   * into a VersionedStore directory whose tagged version is the
   * EXCLUSIVE upper bound on batchID, or "batchID.next".
   */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    val batchVersion = batchIDToVersion(batchID)
    /**
     * The Builder API used to not specify a sinkVersion, leading to
     * versions tagged with the wall clock time. When builder API
     * users migrate over to the new code, they can run into a
     * situation where the new version created has a lower version
     * than the current maximum version in the directory.
     *
     * This behavior clashes with the current VersionedState
     * implementation, which decides what data to source by querying
     * meta.mostRecentVersion. If mostRecentVersion doesn't change
     * from run to run, the job will process the same data over and
     * over.
     *
     * To solve this issue and assist with migrations, if the
     * existing max version in the directory has a timestamp that's
     * greater than that of the batchID being committed, we add a
     * single millisecond to the current version, guaranteeing that
     * we're writing a new max version (but only bumping a tiny bit
     * forward).
     *
     * After a couple of job runs the batchID version should start
     * winning.
     */
    val newVersion: Option[Long] = mode match {
      case m: HdfsMode => {
        val meta = HDFSMetadata(m.conf, rootPath)
        meta.mostRecentVersion.map(_.version)
          .filter(_ > batchVersion)
          .map(_ + 1L)
          .orElse(Some(batchVersion))
      }
      case _ => Some(batchVersion)
    }

    val target = VersionedKeyValSource[K2, V2](rootPath,
      sourceVersion = None,
      sinkVersion = newVersion,
      maxFailures = 0,
      versionsToKeep = versionsToKeep)
    if (!target.sinkExists(mode)) {
      logger.info(s"Versioned batched store version for $this @ $newVersion doesn't exist. Will write out.")
      lastVals.map(pack(batchID, _))
        .write(target)
    } else {
      logger.warn(s"Versioned batched store version for $this @ $newVersion already exists! Will skip adding to plan.")
    }
  }

  /**
   * Returns a FlowProducer that supplies all data for the given
   * specific version within this store's rootPath.
   */
  protected def readVersion(v: Long): FlowProducer[TypedPipe[(K, V)]] = Reader { (flowMode: (FlowDef, Mode)) =>
    val mappable = VersionedKeyValSource[K2, V2](rootPath, sourceVersion = Some(v))
    TypedPipe.from(mappable)
      .map(unpack)
  }
}
