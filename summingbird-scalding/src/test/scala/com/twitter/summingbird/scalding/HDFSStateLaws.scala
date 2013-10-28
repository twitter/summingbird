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

import com.twitter.algebird.Interval
import com.twitter.scalding.{DateParser, RichDate}
import com.twitter.summingbird._
import com.twitter.summingbird.batch._
import com.twitter.summingbird.scalding.state.HDFSState
import java.io.File
import java.util.{ TimeZone, UUID }
import org.apache.hadoop.conf.Configuration
import org.scalacheck.Prop._
import org.scalacheck.Properties
import org.specs2.mutable._

object HDFSStateLaws extends Specification {
  def tempPath: String = {
    val path = "/tmp/" + UUID.randomUUID
    new File(path).deleteOnExit
    path
  }

  def completeState[T](either: Either[WaitingState[T], RunningState[T]]): WaitingState[T] = {
    either match {
      case Right(t) => t.succeed
      case Left(t) => sys.error("PreparedState didn't accept its proposed Interval! failed state: " + t)
    }
  }

  "make sure HDFSState creates checkpoint" in {
    implicit val batcher = Batcher.ofMinutes(30)
    implicit def tz = TimeZone.getTimeZone("UTC")
    implicit def parser = DateParser.default

    val path: String = tempPath
    val startDate: Option[Timestamp] = Some("2012-12-26T09:45").map(RichDate(_).value)
    val numBatches: Long = 10
    val state = HDFSState(path, startTime = startDate, numBatches = numBatches)
    val preparedState = state.begin
    val runningState = preparedState.willAccept(preparedState.requested)
    completeState(runningState)

    BatchID.range(
      batcher.batchOf(startDate.get),
      batcher.batchOf(startDate.get) + numBatches - 1).foreach { t =>
        (path + "/" +
          batcher.earliestTimeOf(t)
          .milliSinceEpoch + ".version") must beAnExistingPath
      }

    //cleanup
    BatchID.range(
      batcher.batchOf(startDate.get),
      batcher.batchOf(startDate.get) + numBatches - 1)
      .foreach { t =>
        new File(path + "/" + batcher.earliestTimeOf(t)
          .milliSinceEpoch + ".version").delete
      }
  }

  "make sure HDFSState creates partial checkpoint" in {
    implicit val batcher: Batcher = new MillisecondBatcher(30 * 60 * 1000L)
    implicit def tz = TimeZone.getTimeZone("UTC")
    implicit def parser = DateParser.default

    val path: String = tempPath
    val startDate: Option[Timestamp] = Some("2012-12-26T09:45").map(RichDate(_).value)
    val numBatches: Long = 10
    val config = HDFSState.Config(path, new Configuration, startDate, numBatches)

    // Not aligned with batch size
    val partialIncompleteInterval: Interval[Timestamp] = Interval.leftClosedRightOpen(
      batcher.earliestTimeOf(batcher.batchOf(startDate.get)),
      RichDate("2012-12-26T10:40").value)

    val partialCompleteInterval: Interval[Timestamp] = Interval.leftClosedRightOpen(
      batcher.earliestTimeOf(batcher.batchOf(startDate.get)),
      RichDate("2012-12-26T11:30").value)

    val runningState = HDFSState(config).begin.willAccept(partialIncompleteInterval)
    val waitingState = runningState match {
      case Left(t) => t
      case Right(t) => sys.error("PreparedState accepted a bad Interval!")
    }

    // reuse the waitingstate
    val waitingState2 = completeState(waitingState.begin.willAccept(partialCompleteInterval))

    BatchID.range(batcher.batchOf(startDate.get), batcher.batchOf(RichDate("2012-12-26T11:30").value) - 1)
      .foreach(t => (path + "/" + batcher.earliestTimeOf(t).milliSinceEpoch + ".version") must beAnExistingPath)

    // start from where you left
    val preparedState2 = waitingState2.begin
    completeState(preparedState2.willAccept(preparedState2.requested))

    BatchID.range(batcher.batchOf(startDate.get), batcher.batchOf(startDate.get) + numBatches - 1)
      .foreach(t => (path + "/" + batcher.earliestTimeOf(t).milliSinceEpoch + ".version") must beAnExistingPath)

    //cleanup
    BatchID.range(batcher.batchOf(startDate.get), batcher.batchOf(startDate.get) + numBatches - 1)
      .foreach(t => new File(path + "/" + batcher.earliestTimeOf(t).milliSinceEpoch + ".version").delete)
  }
}
