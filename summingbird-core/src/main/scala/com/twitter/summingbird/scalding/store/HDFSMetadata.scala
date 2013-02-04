package com.twitter.summingbird.scalding.store

import backtype.hadoop.datastores.{ VersionedStore => BacktypeVersionedStore }
import com.twitter.bijection.json.{ JsonBijection, JsonNodeBijection }
import java.io.{ DataOutputStream, DataInputStream }
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
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

// TODO: Submit a pull req to scalding-commons that adds this generic
// capability to VersionedKeyValSource.

object HDFSMetadata {
  val METADATA_FILE = "_summingbird.json"

  def versionedStore(conf: Configuration, rootPath: String): BacktypeVersionedStore = {
    val fs = FileSystem.get(conf)
    new BacktypeVersionedStore(fs, rootPath)
  }

  def createVersion(conf: Configuration, rootPath: String): String =
    versionedStore(conf, rootPath).createVersion

  def getPath(conf: Configuration, rootPath: String): Option[Path] = {
    val vs = versionedStore(conf, rootPath)
    Option(vs.mostRecentVersionPath) map { new Path(_, METADATA_FILE) }
  }

  def get[T: JsonNodeBijection](conf: Configuration, path: String): Option[T] = {
    getPath(conf, path)
      .map { p =>
        val fs = FileSystem.get(conf)
        val is = new DataInputStream(fs.open(p))
        val ret = JsonBijection.fromString[T](WritableUtils.readString(is))
        is.close
        ret
      }
  }

  def put[T: JsonNodeBijection](conf: Configuration, path: String, obj: Option[T]) {
    val meta = obj
      .map { JsonBijection.toString[T].apply(_) }
      .getOrElse("")

    val fs = FileSystem.get(conf)
    val metaPath = getPath(conf, path).get
    val os = new DataOutputStream(fs.create(metaPath))
    WritableUtils.writeString(os, meta)
    os.close
  }
}
