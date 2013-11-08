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
import com.twitter.summingbird.batch.{ Batcher, BatchID, Timestamp }
import com.twitter.summingbird.scalding.{ WaitingState, PrepareState, RunningState }
import java.io.{ DataOutputStream, DataInputStream }
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import com.backtype.hadoop.datastores.{ VersionedStore => BacktypeVersionedStore }
import com.twitter.scalding.Duration
import com.twitter.bijection.Conversion.asMethod
import com.twitter.algebird.Intersection

case class CheckpointConfig(
    conf: Configuration, 
    rootPath: String, 
    startDate: Option[Timestamp], 
    endDate: Option[Timestamp]) {
}

object CheckpointState {
  def apply(config: CheckpointConfig )(implicit batcher: Batcher) : CheckpointState = new CheckpointState(config)
}

class CheckpointState (config: CheckpointConfig) 
   (implicit batcher: Batcher) extends WaitingState[Interval[Timestamp]] { outer =>
  
  def begin: PrepareState[Interval[Timestamp]] = new CheckpointPrepareState
  
  protected val versionedStore: BacktypeVersionedStore = {
    val fs = FileSystem.get(config.conf)
    new BacktypeVersionedStore(fs, config.rootPath)
  }

  private class CheckpointPrepareState extends PrepareState[Interval[Timestamp]] {
    def startBatch : BatchID = {
      val startTime : Timestamp = config.startDate match {
	    case Some(t) => {
	      if(versionedStore.mostRecentVersion != null) {
	    	  List[Timestamp](t, Timestamp(versionedStore.mostRecentVersion)).max
	      } else {
	        t
	      }
	    }
	    case None => Timestamp(versionedStore.mostRecentVersion)
	  }
	  batcher.batchOf(startTime)
    }
    
    def endBatch : BatchID = {
      val endTime : Timestamp = config.endDate.getOrElse(sys.error("You must supply a ending date on the job's run!"))
	  batcher.batchOf(endTime)
    }
 
    lazy val requested = {
	  // Based on config return the requested interval
      // prepare batches of intervals
	  Interval.leftClosedRightOpen(
        batcher.earliestTimeOf(startBatch),
        batcher.earliestTimeOf(endBatch)
      )
    }
    
    // TODO refactor these and make more functional :(
    def alignsLower(t: Timestamp, inter : Interval[Timestamp]): Boolean = {
      inter.contains(t) && !inter.contains(t.prev)
    }
    
    def alignsUpper(t: Timestamp, inter: Interval[Timestamp]): Boolean = {
      !inter.contains(t) && inter.contains(t.prev)
    }
  
    def willAccept(available: Interval[Timestamp]) = {
      // match one or more batches of intervals
      // If matched 
      //   mark batches of interval as running
      //   return RunningState
      // else return WaitingState
      available match {
        case intr@Intersection(_, _) => {
          val startTime = batcher.earliestTimeOf(startBatch)
          // make sure it matches the startTime
          // We can do this with InclusiveUpper/ExclusiveUpper thingy, but my brain cannot "function" right now.
          // dumb code begins
          if ( alignsLower(startTime, intr) ) {
            BatchID.range(startBatch + 1, endBatch).find(t => alignsUpper(batcher.earliestTimeOf(t), intr)) match {
              case Some(b) => {
                // checkpoint batches until b (exclusive)
                BatchID.range(startBatch, b.prev).foreach(t => versionedStore.succeedVersion(batcher.earliestTimeOf(t).as[Long]));
              }  
              case None => Left(outer)
            }
            Right(new CheckpointRunningState(intr))
          } else {
            Left(outer)
          }
        }
        case _ => Left(outer) 
      }
    }
  
    def fail(err: Throwable) = throw err
  }
  
  private class CheckpointRunningState(succeedPart: Intersection[Timestamp])
    extends RunningState[Interval[Timestamp]] {
    
    def succeed = {
      val nextTime = succeedPart match {
        case Intersection(_, ExclusiveUpper(up)) => up
        case _ => sys.error("We should always be running for a finite interval")
      }
      outer
    }

    def fail(err: Throwable) = {
      // mark the state as failed
      BatchID.toIterable(batcher.batchesCoveredBy(succeedPart)).foreach(t => versionedStore.deleteVersion(batcher.earliestTimeOf(t).as[Long]))
      throw err
    }
  }
}