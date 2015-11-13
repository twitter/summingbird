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

import com.twitter.algebird.bijection.BijectedSemigroup
import com.twitter.algebird.{ Monoid, Semigroup }
import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.algebird.monad.{ StateWithError, Reader }
import com.twitter.bijection.{ Bijection, ImplicitBijection }
import com.twitter.scalding.{ Dsl, Mode, TypedPipe, IterableSource, MapsideReduce, TupleSetter, TupleConverter }
import com.twitter.scalding.typed.Grouped
import com.twitter.summingbird.scalding._
import com.twitter.summingbird.scalding
import com.twitter.summingbird._
import com.twitter.summingbird.option._
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp, IteratorSums, PrunedSpace }
import cascading.flow.FlowDef

import org.slf4j.LoggerFactory

import StateWithError.{ getState, putState, fromEither }
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering

/**
 * This is the same as scala's Tuple2, except the hashCode is a val.
 * We do this so when the tuple2 is placed in the map we won't caculate the hash code
 * unnecessarily several times as the map grows or is transformed.
 */
case class LTuple2[T, U](_1: T, _2: U) {

  override val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)

  override def equals(other: Any): Boolean = other match {
    case LTuple2(oT1, oT2) => hashCode == other.hashCode && _1 == oT1 && _2 == oT2
    case _ => false
  }

}

trait BatchedStore[K, V] extends scalding.Store[K, V] { self =>
  /** The batcher for this store */
  def batcher: Batcher

  implicit def ordering: Ordering[K]

  /**
   * Override select if you don't want to materialize every
   * batch. Note that select MUST return a list containing the final
   * batch in the supplied list; otherwise data would be lost.
   */
  def select(b: List[BatchID]): List[BatchID] = b

  /**
   * Override this to set up store pruning, by default, no (key,value) pairs
   * are pruned. This is a house keeping function to permanently remove entries
   * matching a criteria.
   */
  def pruning: PrunedSpace[(K, V)] = PrunedSpace.neverPruned

  /**
   * For (firstNonZero - 1) we read empty. For all before we error on read. For all later, we proxy
   * On write, we throw if batchID is less than firstNonZero
   */
  def withInitialBatch(firstNonZero: BatchID): BatchedStore[K, V] =
    new scalding.store.InitialBatchedStore(firstNonZero, self)

  /**
   * Get the most recent last batch and the ID (strictly less than the input ID)
   * The "Last" is the stream with only the newest value for each key, within the batch
   * combining the last from batchID and the deltas from batchID.next you get the stream
   * for batchID.next
   */
  def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])]

  /** Record a computed batch of code */
  def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit

  @transient private val logger = LoggerFactory.getLogger(classOf[BatchedStore[_, _]])

  /** The writeLast method as a FlowProducer */
  private def writeFlow(batches: List[BatchID], lastVals: TypedPipe[(BatchID, (K, V))]): FlowProducer[Unit] = {
    logger.info("writing batches: {}", batches)
    Reader[FlowInput, Unit] {
      case (flow, mode) =>
        // make sure we checkpoint to disk to avoid double computation:
        val checked = if (batches.size > 1) lastVals.forceToDisk else lastVals
        batches.foreach { batchID =>
          val thisBatch = checked.filter {
            case (b, kv) =>
              (b == batchID) && !pruning.prune(kv, batcher.latestTimeOf(b))
          }
          writeLast(batchID, thisBatch.values)(flow, mode)
        }
    }
  }

  protected def sumByBatches[K1, V: Semigroup](ins: TypedPipe[(Timestamp, (K1, V))],
    capturedBatcher: Batcher,
    commutativity: Commutativity): TypedPipe[(LTuple2[K1, BatchID], (Timestamp, V))] = {
    implicit val timeValueSemigroup: Semigroup[(Timestamp, V)] =
      IteratorSums.optimizedPairSemigroup[Timestamp, V](1000)

    val inits = ins.map {
      case (t, (k, v)) =>
        val batch = capturedBatcher.batchOf(t)
        (LTuple2(k, batch), (t, v))
    }
    (commutativity match {
      case Commutative => inits.sumByLocalKeys
      case NonCommutative => inits
    })
  }

  /**
   * For each batch, collect up values with the same key on mapside
   * before the keys are expanded.
   */
  override def partialMerge[K1](delta: PipeFactory[(K1, V)],
    sg: Semigroup[V],
    commutativity: Commutativity): PipeFactory[(K1, V)] = {
    logger.info("executing partial merge")
    implicit val semi = sg
    val capturedBatcher = batcher
    commutativity match {
      case Commutative => delta.map { flow =>
        flow.map { typedP =>
          sumByBatches(typedP, capturedBatcher, Commutative)
            .map { case (LTuple2(k, _), (ts, v)) => (ts, (k, v)) }
        }
      }
      case NonCommutative => delta
    }
  }

  /**
   * we are guaranteed to have sufficient input and deltas to cover these batches
   * and that the batches are given in order
   */
  private def mergeBatched(inBatch: BatchID,
    input: FlowProducer[TypedPipe[(K, V)]],
    deltas: FlowToPipe[(K, V)],
    readTimespan: Interval[Timestamp],
    commutativity: Commutativity,
    reducers: Int)(implicit sg: Semigroup[V]): FlowToPipe[(K, (Option[V], V))] = {

    // get the batches read from the readTimespan
    val batchIntr = batcher.batchesCoveredBy(readTimespan)

    val batches = BatchID.toIterable(batchIntr).toList
    val finalBatch = batches.last // batches won't be empty, ensured by atLeastOneBatch method
    val filteredBatches = select(batches).sorted

    assert(filteredBatches.contains(finalBatch), "select must not remove the final batch.")

    import IteratorSums._ // get the groupedSum, partials function

    logger.debug("Previous written batch: {}, computing: {}", inBatch.asInstanceOf[Any], batches)

    def prepareOld(old: TypedPipe[(K, V)]): TypedPipe[(K, (BatchID, (Timestamp, V)))] =
      old.map { case (k, v) => (k, (inBatch, (Timestamp.Min, v))) }

    val capturedBatcher = batcher //avoid a closure on the whole store

    def prepareDeltas(ins: TypedPipe[(Timestamp, (K, V))]): TypedPipe[(K, (BatchID, (Timestamp, V)))] =
      sumByBatches(ins, capturedBatcher, commutativity)
        .map { case (LTuple2(k, batch), (ts, v)) => (k, (batch, (ts, v))) }

    /**
     * Produce a merged stream such that each BatchID, Key pair appears only one time.
     */
    def mergeAll(all: TypedPipe[(K, (BatchID, (Timestamp, V)))]): TypedPipe[(K, (BatchID, (Option[Option[(Timestamp, V)]], Option[(Timestamp, V)])))] = {

      // Make sure to use sumOption on V
      implicit val timeValueSemigroup: Semigroup[(Timestamp, V)] =
        IteratorSums.optimizedPairSemigroup[Timestamp, V](1000)

      val grouped = all.group.withReducers(reducers)

      // We need the tuples to be sorted by batch ID to compute partials, and by time if the
      // monoid is not commutative. Although sorting by time is adequate for both, sorting
      // by batch is more efficient because the reducers' input is almost sorted.
      val sorted = commutativity match {
        case NonCommutative => grouped.sortBy { case (_, (t, _)) => t }(BinaryOrdering.ordSer[com.twitter.summingbird.batch.Timestamp])
        case Commutative => grouped.sortBy { case (b, (_, _)) => b }(BinaryOrdering.ordSer[BatchID])
      }

      sorted
        .mapValueStream { it: Iterator[(BatchID, (Timestamp, V))] =>
          // each BatchID appears at most once, so it fits in RAM
          val batched: Map[BatchID, (Timestamp, V)] = groupedSum(it).toMap
          partials((inBatch :: batches).iterator.map { bid => (bid, batched.get(bid)) })
        }
        .toTypedPipe
    }

    /**
     * There is no flatten on Option, this adds it
     */
    def flatOpt[T](optopt: Option[Option[T]]): Option[T] = optopt.flatMap(identity)

    // This builds the format we write to disk, which is the total sum
    def toLastFormat(res: TypedPipe[(K, (BatchID, (Option[Option[(Timestamp, V)]], Option[(Timestamp, V)])))]): TypedPipe[(BatchID, (K, V))] =
      res.flatMap {
        case (k, (batchid, (prev, v))) =>
          val totalSum = Semigroup.plus[Option[(Timestamp, V)]](flatOpt(prev), v)
          totalSum.map { case (_, sumv) => (batchid, (k, sumv)) }
      }

    // This builds the format we send to consumer nodes
    def toOutputFormat(res: TypedPipe[(K, (BatchID, (Option[Option[(Timestamp, V)]], Option[(Timestamp, V)])))]): TypedPipe[(Timestamp, (K, (Option[V], V)))] =
      res.flatMap {
        case (k, (batchid, (optopt, opt))) =>
          opt.map {
            case (ts, v) =>
              val prev = flatOpt(optopt).map(_._2)
              (ts, (k, (prev, v)))
          }
      }

    // Now in the flow-producer monad; do it:
    for {
      pipeInput <- input
      pipeDeltas <- deltas
      // fork below so scalding can make sure not to do the operation twice
      merged = mergeAll(prepareOld(pipeInput) ++ prepareDeltas(pipeDeltas)).fork
      lastOut = toLastFormat(merged)
      _ <- writeFlow(filteredBatches, lastOut)
    } yield toOutputFormat(merged)
  }

  /**
   * This gives the batches needed to cover the requested input
   * This will always be non-empty
   */
  final def timeSpanToBatches: PlannerOutput[List[BatchID]] = StateWithError({ in: FactoryInput =>
    val (timeSpan, _) = in
    // This object combines some common scalding batching operations:
    val batchOps = new BatchedOperations(batcher)

    (batchOps.coverIt(timeSpan).toList match {
      case Nil => Left(List("Timespan is covered by Nil: %s batcher: %s".format(timeSpan, batcher)))
      case list => Right((in, list))
    })
  })

  /**
   * This is the monadic version of readLast, returns the BatchID actually on disk
   */
  final def planReadLast: PlannerOutput[(BatchID, FlowProducer[TypedPipe[(K, V)]])] =
    for {
      batches <- timeSpanToBatches
      tsMode <- getState[FactoryInput]
      bfp <- fromEither(readLast(batches.min, tsMode._2))
    } yield bfp

  /**
   * Adjist the Lower bound of the interval
   */
  private def setLower(lb: InclusiveLower[Timestamp], interv: Interval[Timestamp]): Interval[Timestamp] = interv match {
    case u @ ExclusiveUpper(_) => lb && u
    case u @ InclusiveUpper(_) => lb && u
    case Intersection(_, u) => lb && u
    case Empty() => Empty()
    case _ => lb // Otherwise the upperbound is infinity.
  }

  /**
   * Reads the input data after the last batch written.
   *
   * Returns:
   * - the BatchID of the last batch written
   * - the snapshot of the store just before this state
   * - the data from this input covering all the time SINCE the last snapshot
   */
  final def readAfterLastBatch[T](input: PipeFactory[T]): PlannerOutput[(BatchID, FlowProducer[TypedPipe[(K, V)]], FlowToPipe[T])] = {
    // StateWithError lacks filter, so it can't unpack tuples (scala limitation)
    // so unfortunately, this code has a lot of manual tuple unpacking for that reason
    for {
      // Get the BatchID and data for the last snapshot,
      bidFp <- planReadLast
      (lastBatch, lastSnapshot) = bidFp

      // Get latest time for the last batch written to store
      lastTimeWrittenToStore = batcher.latestTimeOf(lastBatch)

      // Now get the first timestamp that we need input data for.
      firstDeltaTimestamp = lastTimeWrittenToStore.next

      // Get the requested timeSpan.
      tsMode <- getState[FactoryInput]
      (timeSpan, mode) = tsMode

      // Get the batches covering the requested timeSpan so we can get the last time stamp we need to request.
      batchOps = new BatchedOperations(batcher)

      // Get the total time we want to cover. If the lower bound of the requested timeSpan
      // is not the firstDeltaTimestamp, adjust it to that.
      deltaTimes: Interval[Timestamp] = setLower(InclusiveLower(firstDeltaTimestamp), timeSpan)

      // Try to read the range covering the time we want; get the time we can completely
      // cover and the data from input in that range.
      readTimeFlow <- fromEither(batchOps.readAvailableTimes(deltaTimes, mode, input))

      (readDeltaTimestamps, readFlow) = readTimeFlow

      // Make sure that the time we can read includes the time just after the last
      // snapshot. We can't roll the store forward without this.
      _ <- fromEither[FactoryInput](if (readDeltaTimestamps.contains(firstDeltaTimestamp))
        Right(())
      else
        Left(List("Cannot load initial timestamp " + firstDeltaTimestamp.toString + " of deltas " +
          " at " + this.toString + " only " + readDeltaTimestamps.toString)))

      // Record the timespan we actually read.
      _ <- putState((readDeltaTimestamps, mode))
    } yield (lastBatch, lastSnapshot, readFlow)
  }

  /**
   * This combines the current inputs along with the last checkpoint on disk to get a log
   * of all deltas with a timestamp
   * This is useful to leftJoin against a store.
   * TODO: This should not limit to batch boundaries, the batch store
   * should handle only writing the data for full batches, but we can materialize
   * more data if it is needed downstream.
   * Note: the returned time interval NOT include the time of the snapshot data point
   * (which is exactly 1 millisecond before the start of the interval).
   */
  def readDeltaLog(delta: PipeFactory[(K, V)]): PipeFactory[(K, V)] =
    readAfterLastBatch(delta).map {
      case (actualLast, snapshot, deltaFlow2Pipe) =>
        val snapshotTs = batcher.latestTimeOf(actualLast)
        Scalding.merge(
          snapshot.map { pipe => pipe.map { (snapshotTs, _) } },
          deltaFlow2Pipe)
    }

  /**
   * This is for ensuring there is at least one batch coverd by readTimespan. This is
   *  required by mergeBatched
   */
  private def atLeastOneBatch(readTimespan: Interval[Timestamp]) =
    fromEither[FactoryInput] {
      if (batcher.batchesCoveredBy(readTimespan) == Empty()) {
        Left(List("readTimespan is not convering at least one batch: " + readTimespan.toString))
      } else {
        Right(())
      }
    }

  /**
   * instances of this trait MAY NOT change the logic here. This always follows the rule
   * that we look for existing data (avoiding reading deltas in that case), then we fall
   * back to the last checkpointed output by calling readLast. In that case, we compute the
   * results by rolling forward
   */
  final override def merge(delta: PipeFactory[(K, V)],
    sg: Semigroup[V],
    commutativity: Commutativity,
    reducers: Int): PipeFactory[(K, (Option[V], V))] =
    for {
      // get requested timespan before readAfterLastBatch
      tsModeRequested <- getState[FactoryInput]
      (tsRequested, _) = tsModeRequested

      readBatchedResult <- readAfterLastBatch(delta)
      (actualLast, snapshot, deltaFlow2Pipe) = readBatchedResult

      // get the actual timespan read by readAfterLastBatch
      tsModeRead <- getState[FactoryInput]
      (tsRead, _) = tsModeRead
      _ <- atLeastOneBatch(tsRead)

      /**
       * Once we have read the last snapshot and the available batched blocks of delta, just merge
       */
      merged = mergeBatched(actualLast,
        snapshot,
        deltaFlow2Pipe,
        tsRead,
        commutativity,
        reducers)(sg)

      prunedFlow = Scalding.limitTimes(tsRequested && tsRead, merged)
    } yield (prunedFlow)
}
