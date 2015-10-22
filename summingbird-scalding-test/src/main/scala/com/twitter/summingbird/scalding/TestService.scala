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

import com.twitter.algebird.monad._
import com.twitter.summingbird.batch._

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }
import com.twitter.summingbird.scalding.batch.{ BatchedService => BBatchedService }
import scala.collection.mutable.Buffer
import cascading.tuple.Tuple
import cascading.flow.FlowDef

object TestStoreService {
  def apply[K, V](store: TestStore[K, V]): TestStoreService[K, V] = {
    new TestStoreService(store)
  }
}

class TestStoreService[K, V](store: TestStore[K, V]) extends StoreService[K, V](store) {
  val sourceToBuffer: Map[ScaldingSource, Buffer[Tuple]] = store.sourceToBuffer
}

class TestService[K, V](service: String,
  inBatcher: Batcher,
  minBatch: BatchID,
  streams: Map[BatchID, Iterable[(Timestamp, (K, Option[V]))]])(implicit ord: Ordering[K],
    tset: TupleSetter[(Timestamp, (K, Option[V]))],
    tset2: TupleSetter[(Timestamp, (K, V))],
    tconv: TupleConverter[(Timestamp, (K, Option[V]))],
    tconv2: TupleConverter[(Timestamp, (K, V))])
    extends BBatchedService[K, V] {
  import OrderedFromOrderingExt._
  val batcher = inBatcher
  val ordering = ord
  val reducers = None
  // Needed to init the Test mode:
  val sourceToBuffer: Map[ScaldingSource, Buffer[Tuple]] =
    (lasts.map { case (b, it) => lastMappable(b) -> toBuffer(it) } ++
      streams.map { case (b, it) => streamMappable(b) -> toBuffer(it) }).toMap

  /** The lasts are computed from the streams */
  lazy val lasts: Map[BatchID, Iterable[(Timestamp, (K, V))]] = {
    (streams
      .toList
      .sortBy(_._1)
      .foldLeft(Map.empty[BatchID, Map[K, (Timestamp, V)]]) {
        case (map, (batch: BatchID, writes: Iterable[(Timestamp, (K, Option[V]))])) =>
          val thisBatch = writes.foldLeft(map.get(batch).getOrElse(Map.empty[K, (Timestamp, V)])) {
            case (innerMap, (time, (k, v))) =>
              v match {
                case None => innerMap - k
                case Some(v) => innerMap + (k -> (time -> v))
              }
          }
          map + (batch -> thisBatch)
      }
      .mapValues { innerMap =>
        innerMap.toSeq.map { case (k, (time, v)) => (time, (k, v)) }
      }) + (minBatch -> Iterable.empty)
  }

  def lastMappable(b: BatchID): Mappable[(Timestamp, (K, V))] =
    new MockMappable[(Timestamp, (K, V))](service + "/last/" + b.toString)

  def streamMappable(b: BatchID): Mappable[(Timestamp, (K, Option[V]))] =
    new MockMappable[(Timestamp, (K, Option[V]))](service + "/stream/" + b.toString)

  def toBuffer[T](it: Iterable[T])(implicit ts: TupleSetter[T]): Buffer[Tuple] =
    it.map { ts(_) }.toBuffer

  override def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, Option[V])]] = {
    streams.get(batchID).map { iter =>
      val mappable = streamMappable(batchID)
      Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable) }
    }
  }
  override def readLast(exclusiveUB: BatchID, mode: Mode) = {
    val candidates = lasts.filter { _._1 < exclusiveUB }
    if (candidates.isEmpty) {
      Left(List("No batches < :" + exclusiveUB.toString))
    } else {
      val (batch, _) = candidates.maxBy { _._1 }
      val mappable = lastMappable(batch)
      val rdr = Reader { (fd: (FlowDef, Mode)) =>
        TypedPipe.from(mappable).values
      }
      Right((batch, rdr))
    }
  }
}
