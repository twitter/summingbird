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

import com.twitter.algebird.{ Interval, Intersection, ExclusiveUpper, InclusiveUpper }
import com.twitter.summingbird.batch.{ Batcher, BatchID, Timestamp }
import com.twitter.summingbird.scalding.store.HDFSMetadata

/**
  * State representation used by the builder API for compatibility.
  */
private[scalding] object VersionedState {
  def apply(meta: HDFSMetadata, startDate: Option[Timestamp], maxBatches: Int)
    (implicit batcher: Batcher): VersionedState =
    new VersionedState(meta, startDate, maxBatches)
}

private[scalding] class VersionedState(meta: HDFSMetadata, startDate: Option[Timestamp], maxBatches: Int)
  (implicit batcher: Batcher) extends WaitingState[Interval[Timestamp]] { outer =>

  def begin: PrepareState[Interval[Timestamp]] = new VersionedPrepareState

  private class VersionedPrepareState extends PrepareState[Interval[Timestamp]] {
    /**
      * Returns a date interval spanning from the beginning of the the
      * batch stored in the most recent metadata file to the current
      * time.
      */
    lazy val requested = {
      val beginning: BatchID =
        startDate.map(batcher.batchOf(_))
          .orElse {
          for {
            version <- meta.mostRecentVersion
            batchString <- version.get[String].toOption
          } yield BatchID(batchString)
        } getOrElse {
          sys.error("You must supply a starting date on the job's first run!")
        }
      val end = beginning + maxBatches
      Interval.leftClosedRightOpen(
        batcher.earliestTimeOf(beginning),
        batcher.earliestTimeOf(end)
      )
    }

    def willAccept(available: Interval[Timestamp]) =
      available match {
        case intr@Intersection(_, _) => // is finite:
          Right(new VersionedRunningState(intr))
        case _ => Left(outer)
      }
    /**
      * failure has no side effect, since any job writing to a
      * VersionedStore should itself be cleaning up the most recent
      * failed version's data.
      */
    def fail(err: Throwable) = throw err
  }

  private class VersionedRunningState(succeedPart: Intersection[Timestamp])
    extends RunningState[Interval[Timestamp]] {
    /**
      * Commit the new maximum completed interval to the backing
      * HDFSMetadata instance.
      */
    def succeed = {
      val nextTime = succeedPart match {
        case Intersection(_, ExclusiveUpper(up)) => up
        case Intersection(_, InclusiveUpper(up)) => up.next
        case _ => sys.error("We should always be running for a finite interval")
      }
      val batchID = batcher.batchOf(nextTime)
      meta.mostRecentVersion.foreach(_.put(Some(batchID.toString)))
      outer
    }

    /**
      * failure has no side effect, since any job writing to a
      * VersionedStore should itself be cleaning up the most recent
      * failed version's data.
      */
    def fail(err: Throwable) = throw err
  }
}
