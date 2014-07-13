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

package com.twitter.summingbird.batch.store

import com.backtype.hadoop.datastores.{ VersionedStore => BacktypeVersionedStore }
import com.twitter.bijection.json.{ JsonInjection, JsonNodeInjection }
import java.io.{ DataOutputStream, DataInputStream }
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.Exception.allCatch

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// HDFSMetadata allows metadata about a versioned dataset to be stored
// alongside that version. "versions" are really just timestamp folder
// names guaranteed to appear in increasing order. The BatchIDs stored
// by Summingbird are the exclusive limit on the BatchIDs of data
// processed into that versioned dataset. Put another way, event from
// times
//
// (-INF, BatchID(latestTime)]
//
// is guaranteed to be present in the current dataset. The Summingbird
// client uses this information to determine what batches it needs to
// fetch from the online sink.
//
// HDFSMetadata uses JSON to allow for later extensions to the
// metadata model. One idea is to store a Clojure event filtering
// function as a string (to be eval'd on the server) that the realtime
// layer could use to skip processing of certain events.

// TODO (https://github.com/twitter/summingbird/issues/93): Submit a
// pull req to scalding-commons that adds this generic capability to
// VersionedKeyValSource.

private[summingbird] object HDFSMetadata {
  val METADATA_FILE = "_summingbird.json"

  def apply(conf: Configuration, rootPath: String): HDFSMetadata =
    new HDFSMetadata(conf, rootPath)

  /** Get from the most recent version */
  def get[T: JsonNodeInjection](conf: Configuration, path: String): Option[T] =
    apply(conf, path)
      .mostRecentVersion
      .flatMap { _.get[T].toOption }

  /** Put to the most recent version */
  def put[T: JsonNodeInjection](conf: Configuration, path: String, obj: Option[T]) =
    apply(conf, path)
      .mostRecentVersion
      .get.put(obj)
}

/**
 * Class to access metadata for a single versioned output directory
 * @param rootPath the base root path to where versions of a single output are stored
 */
class HDFSMetadata(conf: Configuration, rootPath: String) {
  protected val versionedStore: BacktypeVersionedStore = {
    val fs = FileSystem.get(conf)
    new BacktypeVersionedStore(fs, rootPath)
  }

  /**
   * path to a specific version WHETHER OR NOT IT EXISTS
   */
  private def pathOf(version: Long): Path =
    new Path(versionedStore.versionPath(version), HDFSMetadata.METADATA_FILE)

  /**
   * The greatest version number that has been completed on disk
   */
  def mostRecentVersion: Option[HDFSVersionMetadata] =
    Option(versionedStore.mostRecentVersion)
      .map { jlong => new HDFSVersionMetadata(jlong.longValue, conf, pathOf(jlong.longValue)) }

  /** Create a new version number that is greater than all previous */
  def newVersion: Long = versionedStore.newVersion

  /** Find the newest version that satisfies a predicate */
  def find[T: JsonNodeInjection](fn: (T) => Boolean): Option[(T, HDFSVersionMetadata)] =
    select(fn).headOption

  /** select all versions that satisfy a predicate */
  def select[T: JsonNodeInjection](fn: (T) => Boolean): Iterable[(T, HDFSVersionMetadata)] =
    for {
      v <- versions
      hmd = apply(v)
      it <- hmd.get[T].toOption if fn(it)
    } yield (it, hmd)

  /**
   * This touches the filesystem once on each call, newest (largest) to oldest (smallest)
   * This relies on dfs-datastore doing the sorting, which it does
   * last we checked
   */
  def versions: Iterable[Long] =
    versionedStore
      .getAllVersions
      .asScala
      .toList
      .sorted
      .reverse
      .map { _.longValue }

  /** Refer to a specific version, even if it does not exist on disk */
  def apply(version: Long): HDFSVersionMetadata =
    new HDFSVersionMetadata(version, conf, pathOf(version))

  /** Get a version's metadata IF it exists on disk */
  def get(version: Long): Option[HDFSVersionMetadata] =
    versions.find { _ == version }.map { apply(_) }
}

/**
 * Refers to a specific version on disk. Allows reading and writing metadata to specific locations
 */
private[summingbird] class HDFSVersionMetadata private[store] (val version: Long, conf: Configuration, val path: Path) {
  private def getString: Try[String] =
    Try {
      val fs = FileSystem.get(conf)
      val is = new DataInputStream(fs.open(path))
      val str = WritableUtils.readString(is)
      is.close
      str
    }
  /**
   * get an item from the metadata file. If there is any failure, you get None.
   */
  def get[T: JsonNodeInjection]: Try[T] =
    getString.flatMap { JsonInjection.fromString[T](_) }

  private def putString(str: String) {
    val fs = FileSystem.get(conf)
    val os = new DataOutputStream(fs.create(path))
    WritableUtils.writeString(os, str)
    os.close
  }

  /** Put a new meta-data file, or overwrite on HDFS */
  def put[T: JsonNodeInjection](obj: Option[T]) = putString {
    obj.map { JsonInjection.toString[T].apply(_) }
      .getOrElse("")
  }
}
