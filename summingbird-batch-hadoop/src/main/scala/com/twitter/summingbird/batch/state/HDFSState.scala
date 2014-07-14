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

import com.twitter.algebird.{ ExclusiveUpper, InclusiveLower, Intersection }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory

/**
 * State implementation that uses an HDFS folder as a crude key-value
 * store that tracks the batches currently processed.
 */
object HDFSState {

  case class Config(rootPath: String,
    conf: Configuration,
    startTime: Option[Timestamp],
    numBatches: Long)

  /**
   * Returns a new HDFSState tuned to the given state path. Each run
   * will fire up at most numBatches batches.
   *
   * Pass in a startTime to restart the process from the specified
   * time.
   */
  def apply(path: String,
    conf: Configuration = new Configuration,
    startTime: Option[Timestamp] = None,
    numBatches: Long = 1)(implicit b: Batcher): HDFSState =
    HDFSState(Config(path, conf, startTime, numBatches))

  /**
   * Returns an HDFSState created directly from a Config object.
   */
  def apply(config: Config)(implicit batcher: Batcher): HDFSState =
    new HDFSState(config)
}

class HDFSState(val config: HDFSState.Config)(implicit val batcher: Batcher)
    extends CheckpointState[Iterable[BatchID]] {
  override val checkpointStore = new HDFSCheckpointStore(config)
}

class HDFSCheckpointStore(val config: HDFSState.Config)(implicit val batcher: Batcher)
    extends CheckpointStore[Iterable[BatchID]] {

  @transient private val logger = LoggerFactory.getLogger(classOf[HDFSState])

  protected lazy val versionedStore =
    new FileVersionTracking(config.rootPath, FileSystem.get(config.conf))

  private def version(b: BatchID) =
    batcher.earliestTimeOf(b).milliSinceEpoch

  val startBatch: InclusiveLower[BatchID] =
    config.startTime.map(batcher.batchOf(_))
      .orElse {
        val mostRecentB = versionedStore.mostRecentVersion
          .map(t => batcher.batchOf(Timestamp(t)).next)
        logger.info("Most recent batch found on disk: " + mostRecentB.toString)
        mostRecentB
      }.map(InclusiveLower(_)).getOrElse {
        sys.error {
          "You must provide startTime in config " +
            "at least for the first run!"
        }
      }

  val endBatch: ExclusiveUpper[BatchID] = ExclusiveUpper(startBatch.lower + config.numBatches)

  override def checkpointBatchStart(intersection: Intersection[InclusiveLower, ExclusiveUpper, Timestamp]): Iterable[BatchID] =
    BatchID.toIterable(batcher.batchesCoveredBy(intersection))

  override def checkpointSuccessfulRun(runningBatches: Iterable[BatchID]) =
    runningBatches.foreach { b => versionedStore.succeedVersion(version(b)) }

  override def checkpointFailure(runningBatches: Iterable[BatchID], err: Throwable) =
    runningBatches.foreach { b => versionedStore.deleteVersion(version(b)) }

  override def checkpointPlanFailure(err: Throwable) = throw err

}
