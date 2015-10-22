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

package com.twitter.summingbird.batch

import java.util.{ TimeZone, UUID }

import com.twitter.algebird.{ Intersection, Interval }
import com.twitter.scalding.{ DateParser, RichDate }
import com.twitter.summingbird.batch.state.HDFSState
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.scalatest.WordSpec

class HDFSStateLaws extends WordSpec {

  val batchLength: Long = 30
  implicit val batcher = Batcher.ofMinutes(batchLength)
  implicit def tz = TimeZone.getTimeZone("UTC")
  implicit def parser = DateParser.default

  "make sure HDFSState creates checkpoint" in {
    withTmpDir { path =>
      val startDate: Timestamp = RichDate("2012-12-26T09:45").value
      val numBatches: Long = 10
      val state = HDFSState(path, startTime = Some(startDate), numBatches = numBatches) //startDate is specified for the first run
      val preparedState = state.begin
      val requested = preparedState.requested

      shouldCheckpointInterval(batcher, state, requested, path)

      //after the first batch, startTime is not needed
      //it should read from the HDFS folder to decide what is the start time
      //put it simply: start from where you left
      val nextState = HDFSState(path, startTime = None, numBatches = numBatches)
      val nextPrepareState = nextState.begin
      nextPrepareState.requested match {
        case intersection @ Intersection(low, high) => {
          val startBatchTime: Timestamp = batcher.earliestTimeOf(batcher.batchOf(startDate))
          val expectedNextRunStartMillis: Long = startBatchTime.incrementMinutes(numBatches * batchLength).milliSinceEpoch
          assert(low.least.get.milliSinceEpoch == expectedNextRunStartMillis)
        }
        case _ => fail("requested interval should be an interseciton")
      }
      shouldCheckpointInterval(batcher, nextState, nextPrepareState.requested, path)
    }
  }

  "make sure HDFSState creates partial checkpoint" in {
    withTmpDir { path =>
      val startDate: Option[Timestamp] = Some("2012-12-26T09:45").map(RichDate(_).value)
      val numBatches: Long = 10
      val config = HDFSState.Config(path, new Configuration, startDate, numBatches)
      val waitingState: HDFSState = HDFSState(config)
      val startBatchTime: Timestamp = batcher.earliestTimeOf(batcher.batchOf(startDate.get))
      // Not aligned with batch size
      val partialIncompleteInterval: Interval[Timestamp] = leftClosedRightOpenInterval(startBatchTime, RichDate("2012-12-26T10:40").value)
      shouldNotAcceptInterval(waitingState, partialIncompleteInterval)

      //artificially create a 1 millis hole
      val intervalWithHoles: Interval[Timestamp] = leftClosedRightOpenInterval(startBatchTime.incrementMillis(1), RichDate("2012-12-26T11:30").value)
      shouldNotAcceptInterval(waitingState, intervalWithHoles)

      val partialCompleteInterval: Interval[Timestamp] = leftClosedRightOpenInterval(startBatchTime, RichDate("2012-12-26T11:30").value)
      shouldCheckpointInterval(batcher, waitingState, partialCompleteInterval, path)
    }
  }

  def leftClosedRightOpenInterval(low: Timestamp, high: Timestamp) = Interval.leftClosedRightOpen[Timestamp](low, high).right.get

  def shouldNotAcceptInterval(state: WaitingState[Interval[Timestamp]], interval: Interval[Timestamp], message: String = "PreparedState accepted a bad Interval!") = {
    state.begin.willAccept(interval) match {
      case Left(t) => t
      case Right(t) => sys.error(message)
    }
  }

  def completeState[T](either: Either[WaitingState[T], RunningState[T]]): WaitingState[T] = {
    either match {
      case Right(t) => t.succeed
      case Left(t) => sys.error("PreparedState didn't accept its proposed Interval! failed state: " + t)
    }
  }

  def shouldCheckpointInterval(batcher: Batcher, state: WaitingState[Interval[Timestamp]], interval: Interval[Timestamp], path: String) = {
    completeState(state.begin.willAccept(interval))
    interval match {
      case intersection @ Intersection(low, high) => {
        BatchID.range(batcher.batchOf(low.least.get), batcher.batchOf(high.greatest.get))
          .foreach { t =>
            val totPath = (path + "/" + batcher.earliestTimeOf(t).milliSinceEpoch + ".version")
            assert(new java.io.File(totPath).exists)
          }
      }
      case _ => sys.error("interval should be an intersection")
    }
  }

  def withTmpDir(doWithTmpFolder: String => Unit) = {
    val path = "/tmp/" + UUID.randomUUID
    try {
      doWithTmpFolder(path)
    } finally {
      FileSystem.get(new Configuration()).delete(new Path(path), true)
    }
  }
}
