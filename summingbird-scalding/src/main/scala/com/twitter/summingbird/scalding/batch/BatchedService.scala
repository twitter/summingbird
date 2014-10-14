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
import com.twitter.algebird.{ Interval, Semigroup }
import com.twitter.scalding.{ Mode, TypedPipe }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.scalding._
import com.twitter.summingbird.scalding
import cascading.flow.FlowDef

trait BatchedService[K, V] extends ExternalService[K, V] {
  // The batcher that describes this service
  def batcher: Batcher
  def ordering: Ordering[K]

  /**
   * Get the most recent last batch and the ID (strictly less than the input ID)
   * The "Last" is the stream with only the newest value for each key.
   */
  def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])]

  /**
   * Reads the key log for this batch. By key log we mean a log of time, key and value when the key was written
   * to. This is an associative operation and sufficient to scedule the service.
   * This only has the keys that changed value during this batch.
   */
  def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, Option[V])]]

  def reducers: Option[Int]

  /**
   * This executes the join algorithm on the streams.
   * You are guaranteed that all the service data needed
   * to do the join is present.
   */
  def lookup[W](incoming: TypedPipe[(Timestamp, (K, W))],
    servStream: TypedPipe[(Timestamp, (K, Option[V]))]): TypedPipe[(Timestamp, (K, (W, Option[V])))] = {

    def flatOpt[T](o: Option[Option[T]]): Option[T] = o.flatMap(identity)

    implicit val ord = ordering
    LookupJoin(incoming, servStream, reducers)
      .map { case (t, (k, (w, optoptv))) => (t, (k, (w, flatOpt(optoptv)))) }
  }

  protected def batchedLookup[W](covers: Interval[Timestamp],
    getKeys: FlowToPipe[(K, W)],
    last: (BatchID, FlowProducer[TypedPipe[(K, V)]]),
    streams: Iterable[(BatchID, FlowToPipe[(K, Option[V])])]): FlowToPipe[(K, (W, Option[V]))] =
    Reader[FlowInput, KeyValuePipe[K, (W, Option[V])]] { (flowMode: (FlowDef, Mode)) =>
      val left = getKeys(flowMode)
      val earliestInLast = batcher.earliestTimeOf(last._1)
      val liftedLast: KeyValuePipe[K, Option[V]] = last._2(flowMode)
        .map { case (k, w) => (earliestInLast, (k, Some(w))) }
      // TODO (https://github.com/twitter/summingbird/issues/91): we
      // could not bother to load streams outside the covers, but
      // probably we aren't anyway assuming the time spans are not
      // wildly mismatched
      val right = streams.foldLeft(liftedLast) { (merged, item) =>
        merged ++ (item._2(flowMode))
      }
      lookup(left, right)
    }

  final def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))] =
    StateWithError({ (in: FactoryInput) =>
      val (timeSpan, mode) = in

      // This object combines some common scalding batching operations:
      val batchOps = new BatchedOperations(batcher)

      val coveringBatches = batchOps.coverIt(timeSpan)
      readLast(coveringBatches.min, mode).right.flatMap { batchLastFlow =>
        val (startingBatch, init) = batchLastFlow
        val streamBatches = BatchID.range(startingBatch.next, coveringBatches.max)
        val batchStreams = streamBatches.map { b => (b, readStream(b, mode)) }
        // only produce continuous output, so stop at the first none:
        val existing = batchStreams
          .takeWhile { _._2.isDefined }
          .collect { case (batch, Some(flow)) => (batch, flow) }

        if (existing.isEmpty) {
          Left(List("[ERROR] Could not load any batches of the service stream in: " + toString + " for: " + timeSpan.toString))
        } else {
          val inBatches = List(startingBatch) ++ existing.map { _._1 }
          val bInt = BatchID.toInterval(inBatches).get // by construction this is an interval, so this can't throw
          val toRead = batchOps.intersect(bInt, timeSpan) // No need to read more than this
          getKeys((toRead, mode))
            .right
            .map {
              case ((available, outM), getFlow) =>
                /*
               * Note we can open more batches than we need to join, but
               * we will deal with that when we do the join (by filtering first,
               * then grouping on (key, batchID) to parallelize.
               */
                ((available, outM),
                  Scalding.limitTimes(available, batchedLookup(available, getFlow, batchLastFlow, existing)))
            }
        }
      }
    })
}

object BatchedService extends java.io.Serializable {
  /**
   * If you write the output of a sumByKey, you can use it as
   * a BatchedService
   * Assumes the batcher is the same for both
   */
  def fromStoreAndSink[K, V](store: BatchedStore[K, V],
    sink: BatchedSink[(K, Option[V])],
    reducerOption: Option[Int] = None): BatchedService[K, V] = new BatchedService[K, V] {
    override def ordering = store.ordering
    override def batcher = {
      assert(store.batcher == sink.batcher, "Batchers do not match")
      store.batcher
    }
    override val reducers = reducerOption
    override def readStream(batchID: BatchID, mode: Mode) =
      sink.readStream(batchID, mode)
    override def readLast(exclusiveUB: BatchID, mode: Mode) =
      store.readLast(exclusiveUB, mode)
  }

  /**
   * If you write the output JUST BEFORE sumByKey, you can use it as
   * a BatchedService
   * Assumes the batcher is the same for both
   */
  def fromStoreAndDeltaSink[K, V: Semigroup](store: BatchedStore[K, V],
    sink: BatchedSink[(K, V)],
    reducerOption: Option[Int] = None): scalding.service.BatchedDeltaService[K, V] =
    new scalding.service.BatchedDeltaService[K, V](store, sink, reducerOption)
}
