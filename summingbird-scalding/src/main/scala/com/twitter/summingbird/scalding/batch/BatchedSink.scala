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

package com.twitter.summingbird.scalding.batch

import com.twitter.algebird.monad.{ StateWithError, Reader }
import com.twitter.algebird.{ Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.scalding._
import com.twitter.scalding.Mode
import cascading.flow.FlowDef

trait BatchedSink[T] extends Sink[T] {
  def batcher: Batcher

  /**
   * If this full stream for this batch is already materialized, return it
   */
  def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[T]]

  /**
   * Instances may choose to write out materialized streams
   * by implementing this. This is what readStream returns.
   */
  def writeStream(batchID: BatchID, stream: TimedPipe[T])(implicit flowDef: FlowDef, mode: Mode): Unit

  /**
   * in will completely cover these batches
   * Return a new FlowToPipe with the write as a side effect
   */
  protected def writeBatches(inter: Interval[BatchID], in: FlowToPipe[T]): FlowToPipe[T] =
    Reader[FlowInput, TimedPipe[T]] { (flowMode: (FlowDef, Mode)) =>
      val iter = BatchID.toIterable(inter)
      val inPipe = in(flowMode)

      // TODO (https://github.com/twitter/summingbird/issues/92): a
      // version of template tap is needed here.

      // We need to write each of these.
      iter.foreach { batch =>
        val range = batcher.toInterval(batch)
        writeStream(batch, inPipe.filter {
          case (time, _) =>
            range(time)
        })(flowMode._1, flowMode._2)
      }
      inPipe
    }

  final def write(incoming: PipeFactory[T]): PipeFactory[T] =
    StateWithError({ in: FactoryInput =>
      val (timeSpan, mode) = in
      // This object combines some common scalding batching operations:
      val batchOps = new BatchedOperations(batcher)

      val batchStreams = batchOps.coverIt(timeSpan).map { b => (b, readStream(b, mode)) }

      // Maybe an inclusive interval of batches to pull from incoming
      val batchesToWrite: Option[(BatchID, BatchID)] = batchStreams
        .dropWhile { _._2.isDefined }
        .map { _._1 }
        .toList match {
          case Nil => None
          case list => Some((list.min, list.max))
        }

      val newlyWritten = batchesToWrite.map {
        case (lower, upper) =>
          // Compute the times we need to read of the deltas
          val incBatches = Interval.leftClosedRightOpen(lower, upper.next)
          batchOps.readBatched(incBatches, mode, incoming)
            .right
            .map { case (inbatches, flow2Pipe) => (inbatches, writeBatches(inbatches, flow2Pipe)) }
      }
      // This data is already on disk and will not be recomputed
      val existing = batchStreams
        .takeWhile { _._2.isDefined }
        .collect { case (batch, Some(flow)) => (batch, flow) }

      def mergeExistingAndBuilt(optBuilt: Option[(Interval[BatchID], FlowToPipe[T])]): Try[((Interval[Timestamp], Mode), FlowToPipe[T])] = {
        val (aBatches, aFlows) = existing.unzip
        val flows = aFlows ++ (optBuilt.map { _._2 })
        val batches = aBatches ++ (optBuilt.map { pair => BatchID.toIterable(pair._1) }.getOrElse(Iterable.empty))

        if (flows.isEmpty)
          Left(List("Zero batches requested, should never occur: " + timeSpan.toString))
        else {
          // it is a static (i.e. independent from input) bug if this get ever throws
          val available = batchOps.intersect(batches, timeSpan).get
          val merged = Scalding.limitTimes(available, flows.reduce(Scalding.merge(_, _)))
          Right(((available, mode), merged))
        }
      }

      newlyWritten match {
        case None => mergeExistingAndBuilt(None)
        case Some(Left(err)) => if (existing.isEmpty) Left(err) else mergeExistingAndBuilt(None)
        case Some(Right(built)) => mergeExistingAndBuilt(Some(built))
      }
    })
}
