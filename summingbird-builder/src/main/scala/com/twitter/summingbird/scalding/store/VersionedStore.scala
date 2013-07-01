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
import com.twitter.bijection.Injection
import com.twitter.scalding.{Dsl, Hdfs, Mode, TDsl, TypedPipe}
import com.twitter.scalding.commons.source.VersionedKeyValSource
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding._
import scala.util.control.Exception.allCatch

/**
  * Scalding implementation of the batch read and write components
  * of a store that uses the VersionedKeyValSource from scalding-commons.
  *
  * @author Oscar Boykin
  * @author Sam Ritchie
  * @author Ashu Singhal
  */

object VersionedStore {
  def apply[K, V](rootPath: String, versionsToKeep: Int = VersionedKeyValSource.defaultVersionsToKeep)
    (implicit injection: Injection[(K, V), (Array[Byte], Array[Byte])]) =
    new VersionedStore[K, V](rootPath, versionsToKeep)
}

// TODO: it looks like when we get the mappable/directory this happens
// at a different time (not atomically) with getting the
// meta-data. This seems like something we need to fix: atomically get
// meta-data and open the Mappable.
// The source parameter is pass-by-name to avoid needing the hadoop
// Configuration object when running the storm job.
class VersionedStore[K, V](rootPath: String, versionsToKeep: Int)(
  implicit injection: Injection[(K, V), (Array[Byte], Array[Byte])])  extends BatchedScaldingStore[K, V] {
  import Dsl._
  import TDsl._

  // ## Batch-write Components

  def prevUpperBound(env: ScaldingEnv): Option[(BatchID, Long)] =
    HDFSMetadata(env.config, rootPath)
      .find { str: String => allCatch.opt(BatchID(str)).isDefined }
      .map { case (str, hdfsVersion) => (BatchID(str), hdfsVersion.version) }

  protected def readVersion(v: Long) =
    VersionedKeyValSource(rootPath, sourceVersion=Some(v))

  override def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode)
      : Option[(BatchID, TypedPipe[(K, V)])] =
    prevUpperBound(env).map { case (bid, version) =>
      (bid, readVersion(version))
    }

  override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowToPipe[(K, V)])] = {
    mode match {
      case Hdfs(_, conf) =>
        HDFSMetadata(conf, rootPath)
          .find { bstr: String => BatchID(bstr) == exclusiveUB }
          .map { case (bstr, hdfsVersion) =>
            val batch = BatchID(bstr)
            val pipe = readVersion(hdfsVersion.version)
            Right[List[FailureReason], T](BatchID(bstr) -> pipe.map { case (k, v) => (1L, k, v) })
        }
      case _ => Left()
    }
  }

  override def availableBatches(env: ScaldingEnv): Iterable[BatchID] =
    HDFSMetadata(env.config, rootPath)
      .select { s: String => true }
      .map { case (batchString, _) => BatchID(batchString) }

  protected var sinkVersion: Option[Long] = None

  // Make sure we only allocate a version once
  def getSinkVersion(env: ScaldingEnv): Long =
    sinkVersion.getOrElse {
      val v = HDFSMetadata(env.config, rootPath).newVersion
      sinkVersion = Some(v)
      v
    }

  def write(env: ScaldingEnv, p: TypedPipe[(K, V)])
    (implicit fd: FlowDef, mode: Mode) {
    p.toPipe((0,1))
      .write(VersionedKeyValSource(rootPath,
        sourceVersion = None,
        sinkVersion = Some(getSinkVersion(env)),
        maxFailures = 0,
        versionsToKeep = versionsToKeep))
  }

  def commit(batchID: BatchID, env: ScaldingEnv) {
    HDFSMetadata(env.config, rootPath)
      .apply(getSinkVersion(env))
      .put(Some(batchID.toString))
  }
}
