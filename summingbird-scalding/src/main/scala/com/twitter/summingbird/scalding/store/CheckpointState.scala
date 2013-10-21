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

import com.twitter.algebird.{Interval, Intersection, ExclusiveUpper, InclusiveUpper }
import com.twitter.summingbird.batch.{ Batcher, BatchID, Timestamp }
import com.twitter.summingbird.scalding.{ WaitingState, PrepareState, RunningState }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import com.backtype.hadoop.datastores.{ VersionedStore => BacktypeVersionedStore }
import com.twitter.bijection.Conversion.asMethod

case class CheckpointConfig(
    conf: Configuration, 
    rootPath: String, 
    startTime: Option[Timestamp], 
    endTime: Option[Timestamp]) {
}

/**
 * Checkpoints every batch between startTime and endTime to make job re-runnable
 * Batcher decides the size of the batches
 * 
 * Assumption: Same batcher is run for multiple runs of the job
 */
object CheckpointState {
  def apply(config: CheckpointConfig )(implicit fixedBatcher: Batcher) : CheckpointState = new CheckpointState(config)
}

class CheckpointState (config: CheckpointConfig) 
   (implicit fixedBatcher: Batcher) extends WaitingState[Interval[Timestamp]] { outer =>
  
  def begin: PrepareState[Interval[Timestamp]] = new CheckpointPrepareState
  
  protected val versionedStore: BacktypeVersionedStore = {
    val fs = FileSystem.get(config.conf)
    new BacktypeVersionedStore(fs, config.rootPath)
  }

  private class CheckpointPrepareState extends PrepareState[Interval[Timestamp]] {
    
    private def startBatch : BatchID = config.startTime match {
	    case Some(t) => {
	      if(versionedStore.mostRecentVersion != null) {
	    	List[BatchID](fixedBatcher.batchOf(t), fixedBatcher.batchOf(Timestamp(versionedStore.mostRecentVersion)) + 1).max
	      } else {
	        fixedBatcher.batchOf(t)
	      }
	    }
	    case None => fixedBatcher.batchOf(Timestamp(versionedStore.mostRecentVersion)) + 1
	  }
    
    private def endBatch : BatchID = {
      val endTimestamp : Timestamp = config.endTime.getOrElse(
        // TODO log warning
        fixedBatcher.earliestTimeOf(startBatch + 1)  
      )
      fixedBatcher.batchOf(endTimestamp)
    }
 
    /**
     * If the batches are running or succeeded as per HDFS checkpoint, start from the next batch
     */
    def requested = {
      Interval.leftClosedRightOpen(
        fixedBatcher.earliestTimeOf(startBatch),
        fixedBatcher.earliestTimeOf(endBatch)
      )
    }
    
    // TODO refactor these and make more functional :(
    private def alignsLower(t: Timestamp, inter : Interval[Timestamp]): Boolean = {
      inter.contains(t) && !inter.contains(t.prev)
    }
    
    private def alignsUpper(t: Timestamp, inter: Interval[Timestamp]): Boolean = {
      !inter.contains(t) && inter.contains(t.prev)
    }
  
    /**
     * only accept interval that aligns size of the batch
     * interval must start from (current) startBatch
     */
    def willAccept(available: Interval[Timestamp]) = {
      available match {
        case intr@Intersection(_, _) => {
          val startTime = fixedBatcher.earliestTimeOf(startBatch)
          
          if ( alignsLower(startTime, intr) ) {
            BatchID.range(startBatch + 1, endBatch).find(t => alignsUpper(fixedBatcher.earliestTimeOf(t), intr)) match {
              case Some(b) => {
                // checkpoint batches until b (exclusive)
                BatchID.range(startBatch, b.prev)
                  .foreach(t => versionedStore.succeedVersion(fixedBatcher.earliestTimeOf(t).as[Long]));
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
      succeedPart match {
        case Intersection(_, ExclusiveUpper(up)) => up
        case _ => sys.error("We should always be running for a finite interval")
      }
      outer
    }

    def fail(err: Throwable) = {
      // mark the state as failed
      BatchID.toIterable(fixedBatcher.batchesCoveredBy(succeedPart))
        .foreach(t => versionedStore.deleteVersion(fixedBatcher.earliestTimeOf(t).as[Long]))
      throw err
    }
  }
}
