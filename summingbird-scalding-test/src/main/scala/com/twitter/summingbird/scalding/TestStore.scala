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

import com.twitter.algebird.Semigroup
import com.twitter.algebird.monad._
import com.twitter.summingbird.batch._
import com.twitter.summingbird.batch.state.HDFSState

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }
import com.twitter.scalding.typed.TypedSink

import scala.collection.mutable.Buffer

import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import cascading.tuple.{ Tuple, TupleEntry }
import cascading.flow.FlowDef

object TestStore {
  def apply[K, V](store: String, inBatcher: Batcher,
    initStore: Iterable[(K, V)], lastTime: Long,
    pruning: PrunedSpace[(K, V)] = PrunedSpace.neverPruned)(implicit ord: Ordering[K], tset: TupleSetter[(K, V)], tconv: TupleConverter[(K, V)]) = {
    val startBatch = inBatcher.batchOf(Timestamp(0)).prev
    val endBatch = inBatcher.batchOf(Timestamp(lastTime)).next
    new TestStore[K, V](store, inBatcher, startBatch, initStore, endBatch, pruning)
  }
}

class TestStore[K, V](store: String, inBatcher: Batcher, val initBatch: BatchID, initStore: Iterable[(K, V)], lastBatch: BatchID, override val pruning: PrunedSpace[(K, V)])(implicit ord: Ordering[K], tset: TupleSetter[(K, V)], tconv: TupleConverter[(K, V)])
    extends batch.BatchedStore[K, V] {
  import OrderedFromOrderingExt._
  var writtenBatches = Set[BatchID](initBatch)
  val batches: Map[BatchID, Mappable[(K, V)]] =
    BatchID.range(initBatch, lastBatch).map { b => (b, mockFor(b)) }.toMap

  // Needed to init the Test mode:
  val sourceToBuffer: Map[ScaldingSource, Buffer[Tuple]] =
    BatchID.range(initBatch, lastBatch).map { b =>
      if (initBatch == b) (batches(b), initStore.map { tset(_) }.toBuffer)
      else (batches(b), Buffer.empty[Tuple])
    }.toMap

  // Call this after you compute to check the results of the
  def lastToIterable: Iterable[(K, V)] =
    sourceToBuffer(batches(writtenBatches.max)).toIterable.map { tup => tconv(new TupleEntry(tup)) }

  val batcher = inBatcher
  val ordering = ord

  def mockFor(b: BatchID): Mappable[(K, V)] =
    new MockMappable(store + b.toString)

  override def readLast(exclusiveUB: BatchID, mode: Mode) = {
    val candidates = writtenBatches.filter { _ < exclusiveUB }
    if (candidates.isEmpty) {
      Left(List("No batches < :" + exclusiveUB.toString))
    } else {
      val batch = candidates.max
      val mappable = batches(batch)
      val rdr = Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable) }
      Right((batch, rdr))
    }
  }
  /** Instances may choose to write out the last or just compute it from the stream */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    val out = batches(batchID)
    lastVals.write(TypedSink[(K, V)](out))
    writtenBatches = writtenBatches + batchID
  }
}
