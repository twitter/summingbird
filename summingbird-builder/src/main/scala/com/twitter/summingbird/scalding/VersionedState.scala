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

import com.twitter.algebird.{
  InclusiveUpper,
  Intersection,
  Interval,
  ExclusiveUpper
}
import com.twitter.summingbird.batch.{
  Batcher,
  BatchID,
  PrepareState,
  RunningState,
  Timestamp,
  WaitingState
}
import com.twitter.summingbird.batch.store.HDFSMetadata

import org.slf4j.LoggerFactory

import scala.util.{ Try => ScalaTry, Success, Failure }

/**
 * State representation used by the builder API for compatibility.
 */
private[scalding] object VersionedState {
  def apply(meta: HDFSMetadata, startDate: Option[Timestamp], maxBatches: Int)(implicit batcher: Batcher): VersionedState =
    new VersionedState(meta, startDate, maxBatches)
}

private[scalding] class VersionedState(meta: HDFSMetadata, startDate: Option[Timestamp], maxBatches: Int)(implicit batcher: Batcher) extends WaitingState[Interval[Timestamp]] { outer =>

  private val logger = LoggerFactory.getLogger(classOf[VersionedState])

  def begin: PrepareState[Interval[Timestamp]] = new VersionedPrepareState

  private class VersionedPrepareState extends PrepareState[Interval[Timestamp]] {
    def newestCompleted: Option[BatchID] =
      meta.versions.map { vers =>
        val thisMeta = meta(vers)
        thisMeta
          .get[String]
          .flatMap { str => ScalaTry(BatchID(str)) } match {
            case Success(batchID) => Some(batchID)
            case Failure(ex) =>
              logger.warn("Path: {} missing or corrupt completion file. Ignoring and trying previous",
                thisMeta.path)
              None
          }
      }
        .flatten
        .headOption
    /**
     * Returns a date interval spanning from the beginning of the the
     * batch stored in the most recent metadata file to the current
     * time.
     */
    lazy val requested: Interval[Timestamp] = {
      val beginning: BatchID =
        startDate.map(batcher.batchOf(_))
          .orElse(newestCompleted)
          .getOrElse { sys.error("You must supply a starting date on the job's first run!") }

      val end = beginning + maxBatches
      Interval.leftClosedRightOpen(
        batcher.earliestTimeOf(beginning),
        batcher.earliestTimeOf(end)
      ).right.get
    }

    def willAccept(available: Interval[Timestamp]) =
      available match {
        case intr @ Intersection(_, _) => // is finite:
          Right(new VersionedRunningState(intr))
        case _ => {
          logger.info("Will not accept: %s".format(available))
          Left(outer)
        }
      }
    /**
     * failure has no side effect, since any job writing to a
     * VersionedStore should itself be cleaning up the most recent
     * failed version's data.
     */
    def fail(err: Throwable) = {
      logger.error("Prepare failed", err)
      throw err
    }
  }

  private class VersionedRunningState(succeedPart: Interval.GenIntersection[Timestamp])
      extends RunningState[Interval[Timestamp]] {

    def nextTime: Timestamp = succeedPart match {
      case Intersection(_, ExclusiveUpper(up)) => up
      case Intersection(_, InclusiveUpper(up)) => up.next
      case _ => sys.error("We should always be running for a finite interval")
    }
    def batchID: BatchID = batcher.batchOf(nextTime)
    /**
     * Commit the new maximum completed interval to the backing
     * HDFSMetadata instance.
     */
    def succeed = {
      logger.info("%s succeeded".format(batchID))
      meta.mostRecentVersion.foreach(_.put(Some(batchID.toString)))
      outer
    }

    /**
     * failure has no side effect, since any job writing to a
     * VersionedStore should itself be cleaning up the most recent
     * failed version's data.
     */
    def fail(err: Throwable) = {
      logger.error("%s failed".format(batchID), err)
      throw err
    }
  }
}
