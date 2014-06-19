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

package com.twitter.summingbird.batch.state

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import scala.util.{ Try, Success, Failure }

import org.slf4j.LoggerFactory

private[summingbird] object FileVersionTracking {
  @transient private val logger = LoggerFactory.getLogger(classOf[FileVersionTracking])
  val FINISHED_VERSION_SUFFIX = ".version"
  implicit def path(strPath: String): Path = new Path(strPath)
  def path(basePath: String, fileName: String): Path = new Path(basePath, fileName)
}

private[summingbird] case class FileVersionTracking(root: String, fs: FileSystem) {
  import FileVersionTracking._

  fs.mkdirs(root)

  def mostRecentVersion: Option[Long] = getAllVersions.headOption

  def failVersion(version: Long) = deleteVersion(version)

  def deleteVersion(version: Long) = fs.delete(tokenPath(version), false)

  def succeedVersion(version: Long) = fs.createNewFile(tokenPath(version))

  private def getOnDiskVersions: List[Try[Long]] =
    listDir(root)
      .filter(p => !(p.getName.startsWith("_")))
      .filter(_.getName().endsWith(FINISHED_VERSION_SUFFIX))
      .map(f => parseVersion(f.toString()))

  private def logVersion(v: Try[Long]) = logger.debug("Version on disk : " + v.toString)

  def getAllVersions: List[Long] =
    getOnDiskVersions
      .map { v =>
        logVersion(v)
        v
      }
      .collect { case Success(s) => s }
      .sorted
      .reverse

  def hasVersion(version: Long) = getAllVersions.contains(version)

  def tokenPath(version: Long): Path =
    path(root, version.toString + FINISHED_VERSION_SUFFIX)

  def parseVersion(p: String): Try[Long] =
    Try(p.getName().dropRight(FINISHED_VERSION_SUFFIX.length()).toLong)

  def listDir(dir: String): List[Path] = fs.listStatus(dir).map(_.getPath).toList
}