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
    numBatches: Option[Long]) {
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
   (implicit fixedBatcher: Batcher) extends WaitingState[Interval[Timestamp]] {
  
  def begin: PrepareState[Interval[Timestamp]] = new CheckpointPrepareState
  
  protected val versionedStore: BacktypeVersionedStore = {
    val fs = FileSystem.get(config.conf)
    new BacktypeVersionedStore(fs, config.rootPath)
  }

  private class CheckpointPrepareState extends PrepareState[Interval[Timestamp]] {
    
    private lazy val startBatch : BatchID = {
      config.startTime.map(fixedBatcher.batchOf(_))
        .orElse { 
          Option(versionedStore.mostRecentVersion)
          .map(t => fixedBatcher.batchOf(Timestamp(t)).next)
        }
        .getOrElse {sys.error("You must provide startTime in config at least for the first run!")}
	}
    
    private lazy val endBatch : BatchID = {
      startBatch + config.numBatches.getOrElse {println("Number of batches not specified, defaulting to 1"); 1L}
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
                Right(new CheckpointRunningState(intr))
              }
              case None => Left(CheckpointState(config))
            }
          } else {
            Left(CheckpointState(config))
          }
        }
        case _ => Left(CheckpointState(config))
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
      CheckpointState(config)
    }

    def fail(err: Throwable) = {
      // mark the state as failed
      BatchID.toIterable(fixedBatcher.batchesCoveredBy(succeedPart))
        .foreach(t => versionedStore.deleteVersion(fixedBatcher.earliestTimeOf(t).as[Long]))
      throw err
    }
  }
}