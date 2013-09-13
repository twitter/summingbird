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

import com.twitter.algebird.{
  Interval, Intersection, ExclusiveUpper, InclusiveUpper }
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.{ WaitingState, RunningState }

import java.util.Date

/**
  * State representation used by the builder API for compatibility.
  */
object VersionedState {
  def apply(meta: HDFSMetadata, startDate: Option[Date], maxBatches: Int)
    (implicit batcher: Batcher): VersionedState =
    new VersionedState(meta, startDate, maxBatches)
}

class VersionedState(meta: HDFSMetadata, startDate: Option[Date], maxBatches: Int)
  (implicit batcher: Batcher) extends WaitingState[Date] { outer =>
  def begin: RunningState[Date] = new VersionedRunningState

  private class VersionedRunningState extends RunningState[Date] {
    /**
      * Returns a date interval spanning from the beginning of the the
      * batch stored in the most recent metadata file to the current
      * time.
      */
    def part = {
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

    /**
      * Commit the new maximum completed interval to the backing
      * HDFSMetadata instance.
      */
    def succeed(succeedPart: Interval[Date]) = {
      val nextTime = succeedPart match {
        case Intersection(_, ExclusiveUpper(up)) => up
        case Intersection(_, InclusiveUpper(up)) => new Date(up.getTime + 1L)
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
