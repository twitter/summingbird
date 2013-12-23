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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.Writable
import cascading.flow.FlowDef
import cascading.tuple.Fields

import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.algebird.monad.{StateWithError, Reader}
import com.twitter.bijection.{ Bijection, ImplicitBijection }
import com.twitter.scalding.{Dsl, Mode, Hdfs, TypedPipe, IterableSource, WritableSequenceFile, MapsideReduce, TupleSetter, TupleConverter}
import com.twitter.scalding.typed. TypedSink
import com.twitter.summingbird._
import com.twitter.summingbird.option._
import com.twitter.summingbird.batch.{ BatchID, Batcher}

/**
 * DirectoryBatched Scalding Store, which only contains (K, V) data pairs in the data.
 * Batch information is presented in directory pathes.
 *
 * @author Kevin Lin
 */

class DirectoryBatchedStore[K <: Writable, V <: Writable](val rootPath: String)
(implicit inBatcher: Batcher, ord: Ordering[K], tset: TupleSetter[(K, V)], tconv: TupleConverter[(K, V)])
  extends BatchedScaldingStore[K, V] {
  import Dsl._  

  val batcher = inBatcher
  val ordering = ord

  protected def getFileStatus(p: String, conf: Configuration) = {
    val path = new Path(p)
    val (isGood, lastModifyTime): (Boolean, Long) = 
      Option(path.getFileSystem(conf).globStatus(path))
        .map { statuses: Array[FileStatus] =>
          // Must have a file that is called "_SUCCESS"
          val isGood = statuses.exists { fs: FileStatus =>
            fs.getPath.getName == "_SUCCESS"
          }
          val lastModifyTime = statuses.map{_.getModificationTime}.max
          (isGood, lastModifyTime)  
      }
      .getOrElse((false, 0))
      
      (isGood, lastModifyTime, path.getName) 
  }

  /*
   * Get last BatchID for this store
   */
  protected def getLastBatchID(exclusiveUB: BatchID, mode: Mode) = {
    mode match {
      case Hdfs(_, conf) => {
        def hdfsPaths: List[String] = {
          val path = new Path(rootPath)
          Option(path.getFileSystem(conf).globStatus(path))
            .map{ statuses: Array[FileStatus] =>
              statuses.map {_.getPath.getName}
            }
            .getOrElse(Array[String]())
            .toList
        }

        val lastBatchStatus = 
          hdfsPaths.map(getFileStatus(_, conf))
            .filter{input => input._1 && BatchID(input._2) < exclusiveUB}
            .reduceOption{(a, b) => if (a._2 > b._2) a else b}
            .getOrElse((false, 0, "0"))
            
        if (lastBatchStatus._1) BatchID(lastBatchStatus._3) 
        else throw new Exception(
           "No good data <= " + exclusiveUB + " is available at : " + rootPath)
      }
      case _ => {
         throw new Exception(
           "DirectoryBatchedStore must work in Hdfs. Mode: " + mode.toString + " found.")
      } 
    }
  }

  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode) = {
    val outSource = WritableSequenceFile(rootPath + "/" + batchID.toString, 'key -> 'val)
    lastVals.write(TypedSink[(K, V)](outSource))
  }
  
  override def readLast(exclusiveUB: BatchID, mode: Mode) = {
    val lastID = getLastBatchID(exclusiveUB, mode)
    
    val src = WritableSequenceFile(rootPath + "/" + lastID.toString, 'key -> 'val)
    val rdr = Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(src.read(fd._1, fd._2), Fields.ALL)}
    Right((lastID, rdr))
    
  }
}
