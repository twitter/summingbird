package com.twitter.summingbird.scalding.state

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}

import org.slf4j.LoggerFactory

object FileVersionTracking {
  @transient private val logger = LoggerFactory.getLogger(classOf[FileVersionTracking])
  val FINISHED_VERSION_SUFFIX = ".version"
  implicit def path(strPath: String): Path = new Path(strPath)
}

case class FileVersionTracking(root: String, fs: FileSystem) {
    import FileVersionTracking._

    mkdirs(root)

    def mostRecentVersion: Option[Long] = getAllVersions.headOption

    def failVersion(version: Long) = deleteVersion(version)

    def deleteVersion(version: Long) = fs.delete(new Path(tokenPath(version)), false)

    def succeedVersion(version: Long) = createNewFile(tokenPath(version))

    def cleanup(versionsToKeep: Int) {
        val versions = getAllVersions
        val keepers = versions.takeRight(versionsToKeep).toSet
        versions
          .filter(keepers.contains(_))
          .foreach(deleteVersion(_))
    }

    private def getOnDiskVersions: List[Try[Long]] =
        if (fs.exists(root))
          listDir(root)
            .filter(p => !(p.getName.startsWith("_")))
            .filter(_.getName().endsWith(FINISHED_VERSION_SUFFIX))
            .map(f => parseVersion(f.toString()))
        else List()

    private def logVersion(v: Try[Long]) = logger.debug("Version on disk : " + v.toString)

    def getAllVersions: List[Long] =
      getOnDiskVersions
        .map{v =>
          logVersion(v)
          v
        }
        .collect{case Success(s) => s}
        .sorted
        .reverse

    def hasVersion(version: Long) = getAllVersions.contains(version)

    def tokenPath(version: Long): String = tokenPath(version.toString)
    def tokenPath(version: String): String  = (new Path(root, "" + version + FINISHED_VERSION_SUFFIX)).toString()

    def parseVersion(p: String): Try[Long] =
        try {
            Success(p.getName().dropRight(FINISHED_VERSION_SUFFIX.length()).toLong)
        } catch {
          case e: Throwable => Failure(e)
        }

    def createNewFile(p: String) = fs.createNewFile(p)

    def mkdirs(p: String) = fs.mkdirs(p)

    def listDir(dir: String): List[Path] = fs.listStatus(dir).map(_.getPath).toList
}