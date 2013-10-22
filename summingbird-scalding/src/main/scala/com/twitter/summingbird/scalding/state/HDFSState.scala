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
package com.twitter.summingbird.scalding.state

import com.backtype.hadoop.datastores.{ VersionedStore => BacktypeVersionedStore }
import com.twitter.algebird.{ ExclusiveUpper, Intersection, Interval }
import com.twitter.bijection.Conversion.asMethod
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.scalding.{ PrepareState, RunningState, WaitingState, Scalding }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import Scalding.dateRangeInjection

/**
 * Checkpoints every batch between startTime and endTime to make job re-runnable
 * Batcher decides the size of the batches
 *
 * Assumption: Same batcher is run for multiple runs of the job
 */
object HDFSState {
  case class Config(
    conf: Configuration,
    rootPath: String,
    startTime: Option[Timestamp],
    numBatches: Long)

  /**
   * Returns a new HDFSState tuned to the given state path. Each run
   * will fire up at most numBatches batches.
   *
   * Pass in a startTime to restart the process from the specified
   * time.
   */
  def apply(
    path: String,
    conf: Configuration = new Configuration,
    startTime: Option[Timestamp] = None,
    numBatches: Long = 1)(implicit b: Batcher): HDFSState =
    HDFSState(Config(conf, path, startTime, numBatches))

  /**
   * Returns an HDFSState created directly from a Config object.
   */
  def apply(config: Config)(implicit batcher: Batcher): HDFSState =
    new HDFSState(config)
}

class HDFSState(config: HDFSState.Config)(implicit batcher: Batcher)
  extends WaitingState[Interval[Timestamp]] {

  def begin: PrepareState[Interval[Timestamp]] = new Prep

  protected lazy val versionedStore =
    new BacktypeVersionedStore(
      FileSystem.get(config.conf),
      config.rootPath)

  private class Prep extends PrepareState[Interval[Timestamp]] {
    private lazy val startBatch: BatchID =
      config.startTime.map(batcher.batchOf(_))
        .orElse {
          Option(versionedStore.mostRecentVersion)
            .map(t => batcher.batchOf(Timestamp(t)).next)
        }.getOrElse {
          sys.error {
            "You must provide startTime in config at least for the first run!"
          }
        }

    private lazy val startTime = batcher.earliestTimeOf(startBatch)
    private lazy val endBatch: BatchID = startBatch + config.numBatches

    /**
     * If the batches are running or succeeded as per HDFS checkpoint,
     * start from the next batch
     */
    override def requested =
      Interval.leftClosedRightOpen(
        batcher.earliestTimeOf(startBatch),
        batcher.earliestTimeOf(endBatch)
      )

    private def alignsLower(t: Timestamp, inter: Interval[Timestamp]) =
      inter.contains(t) && !inter.contains(t.prev)

    private def alignsUpper(t: Timestamp, inter: Interval[Timestamp]) =
      !inter.contains(t) && inter.contains(t.prev)

    /**
     * only accept interval that aligns size of the batch
     * interval must start from (current) startBatch
     */
    override def willAccept(available: Interval[Timestamp]) =
      Some(available).collect {
        case intr @ Intersection(_, _) if alignsLower(startTime, intr) =>
          BatchID.range(startBatch.next, endBatch)
            .find { t => alignsUpper(batcher.earliestTimeOf(t), intr) }
            .map { _ => Right(new Running(intr)) }
      }.flatMap(identity)
        .getOrElse(Left(HDFSState(config)))

    override def fail(err: Throwable) = throw err
  }

  private class Running(succeedPart: Intersection[Timestamp])
      extends RunningState[Interval[Timestamp]] {
    private lazy val runningBatches =
      BatchID.toIterable(batcher.batchesCoveredBy(succeedPart))

    private def version(b: BatchID) =
      batcher.earliestTimeOf(b).milliSinceEpoch

    /**
      * On success, mark a successful version for every BatchID.
      */
    def succeed = {
      // TODO: Why did I have to delete that other bullshit
      runningBatches.foreach { b => versionedStore.succeedVersion(version(b)) }
      HDFSState(config)
    }

    /**
      * On failure, nuke a version for every BatchID. (None of these
      * should exist on the filesystem, since only one process should
      * be running at a time.)
      */
    def fail(err: Throwable) = {
      // mark the state as failed
      runningBatches.foreach { b => versionedStore.deleteVersion(version(b)) }
      throw err
    }
  }
}
