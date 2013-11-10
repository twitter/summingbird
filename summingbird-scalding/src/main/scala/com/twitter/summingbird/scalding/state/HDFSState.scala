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

import com.twitter.algebird.ExclusiveLower
import com.twitter.algebird.InclusiveLower
import com.twitter.algebird.InclusiveUpper
import com.twitter.algebird.Lower
import com.twitter.algebird.Upper
import com.twitter.algebird.{ ExclusiveUpper, Intersection, Interval }
import com.twitter.bijection.Conversion.asMethod
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.scalding.{ PrepareState, RunningState, WaitingState, Scalding }
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import Scalding.dateRangeInjection

import org.slf4j.LoggerFactory
/**
 * State implementation that uses an HDFS folder as a crude key-value
 * store that tracks the batches currently processed.
 */
object HDFSState {
  @transient private val logger = LoggerFactory.getLogger(classOf[HDFSState])

  case class Config(
    rootPath: String,
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
  def apply(
    path: String,
    conf: Configuration = new Configuration,
    startTime: Option[Timestamp] = None,
    numBatches: Long = 1)(implicit b: Batcher): HDFSState =
    HDFSState(Config(path, conf, startTime, numBatches))

  /**
   * Returns an HDFSState created directly from a Config object.
   */
  def apply(config: Config)(implicit batcher: Batcher): HDFSState =
    new HDFSState(config)

  /**
   * Returns true if each timestamp represents the first
   * millisecond of a batch.
    */
  def alignedToBatchBoundaries(
    low: Lower[Timestamp],
    high: Upper[Timestamp])(implicit b: Batcher): Boolean =
    b.isLowerBatchEdge(toInclusiveLower(low)) &&
    b.isLowerBatchEdge(toExclusiveUpper(high))

  def toInclusiveLower(low: Lower[Timestamp]): Timestamp =
    low match {
      case InclusiveLower(lb) => lb
      case ExclusiveLower(lb) => lb.next
    }

  def toExclusiveUpper(high: Upper[Timestamp]): Timestamp =
    high match {
      case InclusiveUpper(hb) => hb.next
      case ExclusiveUpper(hb) => hb
    }
}

class HDFSState(config: HDFSState.Config)(implicit batcher: Batcher)
    extends WaitingState[Interval[Timestamp]] {
  import HDFSState._

  private val hasStarted = new AtomicBoolean(false)

  def begin: PrepareState[Interval[Timestamp]] = new Prep

  protected lazy val versionedStore =
    new FileVersionTracking(config.rootPath, FileSystem.get(config.conf))

  private class Prep extends PrepareState[Interval[Timestamp]] {
    private lazy val startBatch: InclusiveLower[BatchID] =
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

    private lazy val earliestTimestamp = batcher.earliestTimeOf(startBatch.lower)
    private lazy val endBatch: ExclusiveUpper[BatchID] =
      ExclusiveUpper(startBatch.lower + config.numBatches)

    override def requested =
      Intersection(startBatch, endBatch)
        .mapNonDecreasing(batcher.earliestTimeOf(_))

    def fitsCurrentBatchStart(low: Lower[Timestamp]): Boolean =
      Equiv[Timestamp].equiv(toInclusiveLower(low), earliestTimestamp)

    /**
     * only accept interval that aligns size of the batch interval
     * must start from (current) startBatch
     */
    override def willAccept(available: Interval[Timestamp]) =
      available match {
        case intersection @ Intersection(low, high)
            if (alignedToBatchBoundaries(low, high) &&
              fitsCurrentBatchStart(low) &&
              hasStarted.compareAndSet(false, true)) =>
          Right(new Running(intersection, hasStarted))
        case _ => Left(HDFSState(config))
      }

    override def fail(err: Throwable) = throw err
  }

  private class Running(succeedPart: Intersection[Timestamp], bool: AtomicBoolean)
    extends RunningState[Interval[Timestamp]] {
    def setStopped = assert(
      bool.compareAndSet(true, false),
      "Concurrent modification of HDFSState!"
    )
    private lazy val runningBatches =
      BatchID.toIterable(batcher.batchesCoveredBy(succeedPart))

    private def version(b: BatchID) =
      batcher.earliestTimeOf(b).milliSinceEpoch

    /**
     * On success, mark a successful version for every BatchID.
     */
    def succeed = {
      setStopped
      runningBatches.foreach { b => versionedStore.succeedVersion(version(b)) }
      HDFSState(config)
    }

    /**
     * On failure, nuke a version for every BatchID. (None of these
     * should exist on the filesystem, since only one process should
     * be running at a time.)
     */
    def fail(err: Throwable) = {
      // mark the state as failed. This is defensive, as no one should
      // have created ANYTHING inside of these folders.
      setStopped
      runningBatches.foreach { b => versionedStore.deleteVersion(version(b)) }
      throw err
    }
  }
}
