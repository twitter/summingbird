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

import com.twitter.algebird.{MapAlgebra, Monoid, Group}
import com.twitter.algebird.monad._
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher, Interval}

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap => MutableHashMap, Map => MutableMap, SynchronizedBuffer, SynchronizedMap}

import cascading.scheme.local.{TextDelimited => CLTextDelimited}
import cascading.tuple.{Tuple, Fields, TupleEntry}
import cascading.flow.FlowDef

/**
  * Tests for Summingbird's Scalding planner.
  */

class MockMappable[T](val id: String)(implicit tconv: TupleConverter[T])
    extends ScaldingSource with Mappable[T] {
  val converter = tconv
  override def toString = id
  override def equals(that: Any) = that match {
    case m: MockMappable[_] => m.id == id
    case _ => false
  }
  override def hashCode = id.hashCode

  override def localScheme : LocalScheme = {
    // This is a hack because the MemoryTap doesn't actually care what
    // the scheme is it just holds the fields
    //
    // TODO implement a proper Scheme for MemoryTap
    new CLTextDelimited(sourceFields, "\t", null : Array[Class[_]])
  }
}


class TestStore[K, V](store: String, inBatcher: Batcher, initBatch: BatchID, initStore: Iterable[(K, V)], lastBatch: BatchID)
(implicit ord: Ordering[K], tset: TupleSetter[(K, V)], tconv: TupleConverter[(K, V)])
  extends BatchedScaldingStore[K, V] {

  val batches: Map[BatchID, Mappable[(K, V)]] =
    BatchID.range(initBatch, lastBatch).map { b => (b, mockFor(b)) }.toMap

  // Needed to init the Test mode:
  val sourceToBuffer: Map[ScaldingSource, Buffer[Tuple]] =
    BatchID.range(initBatch, lastBatch).map { b =>
      if (initBatch == b) (batches(b), initStore.map { tset(_) }.toBuffer)
      else (batches(b), Buffer.empty[Tuple])
    }.toMap

  // Call this after you compute to check the results of the
  def lastToIterable(b: BatchID): Iterable[(K, V)] =
    sourceToBuffer(batches(b)).toIterable.map { tup => tconv(new TupleEntry(tup)) }

  val batcher = inBatcher
  val ordering = ord

  def mockFor(b: BatchID): Mappable[(K, V)] =
    new MockMappable(store + b.toString)

  override def readLast(exclusiveUB: BatchID, mode: Mode) = {
    val candidates = batches.filter { _._1 < exclusiveUB }
    if(candidates.isEmpty) {
      Left(List("No batches < :" + exclusiveUB.toString))
    }
    else {
      val (batch, mappable) = candidates.maxBy { _._1 }
      val rdr = Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable)(fd._1, fd._2, mappable.converter) }
      Right((batch, rdr))
    }
  }
  /** Instances may choose to write out the last or just compute it from the stream */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    val out = batches(batchID)
    lastVals.write(out)
  }
}

class TestService[K, V](service: String,
  inBatcher: Batcher,
  lasts: Map[BatchID, Iterable[(Time, (K, V))]],
  streams: Map[BatchID, Iterable[(Time, (K, Option[V]))]])
(implicit ord: Ordering[K],
  tset: TupleSetter[(Time, (K, Option[V]))],
  tset2: TupleSetter[(Time, (K, V))],
  tconv: TupleConverter[(Time, (K, Option[V]))],
  tconv2: TupleConverter[(Time, (K, V))])
  extends BatchedService[K, V] {

  val batcher = inBatcher
  val ordering = ord
  val reducers = None
  // Needed to init the Test mode:
  val sourceToBuffer: Map[ScaldingSource, Buffer[Tuple]] =
    (lasts.map { case (b, it) => lastMappable(b) -> toBuffer(it) } ++
      streams.map { case (b, it) => streamMappable(b) -> toBuffer(it) }).toMap

  def lastMappable(b: BatchID): Mappable[(Time, (K, V))] =
    new MockMappable[(Time, (K, V))](service + "/last/" + b.toString)

  def streamMappable(b: BatchID): Mappable[(Time, (K, Option[V]))] =
    new MockMappable[(Time, (K, Option[V]))](service + "/stream/" + b.toString)

  def toBuffer[T](it: Iterable[T])(implicit ts: TupleSetter[T]): Buffer[Tuple] =
    it.map { ts(_) }.toBuffer

  override def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, Option[V])]] = {
    streams.get(batchID).map { iter =>
      val mappable = streamMappable(batchID)
      Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable)(fd._1, fd._2, mappable.converter) }
    }
  }
  override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowToPipe[(K, V)])] = {
    val candidates = lasts.filter { _._1 < exclusiveUB }
    if(candidates.isEmpty) {
      Left(List("No batches < :" + exclusiveUB.toString))
    }
    else {
      val (batch, _) = candidates.maxBy { _._1 }
      val mappable = lastMappable(batch)
      val rdr = Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable)(fd._1, fd._2, mappable.converter) }
      Right((batch, rdr))
    }
  }
}

object ScaldingLaws extends Properties("Scalding") {
  import MapAlgebra.sparseEquiv

  def testSource[T](iter: Iterable[T])
    (implicit mf: Manifest[T], te: TimeExtractor[T], tc: TupleConverter[T], tset: TupleSetter[T]):
    (Map[ScaldingSource, Buffer[Tuple]], Producer[Scalding, T]) = {
      val src = IterableSource(iter)
      val prod = Scalding.sourceFromMappable { _ => src }
      (Map(src -> iter.map { tset(_) }.toBuffer), prod)
  }

  implicit def tupleExtractor[T <: (Long, _)]: TimeExtractor[T] = TimeExtractor( _._1 )

  val simpleBatcher = new Batcher {
    def batchOf(d: java.util.Date) =
      if (d.getTime == Long.MaxValue) BatchID(2)
      else if (d.getTime >= 0L) BatchID(1)
      else BatchID(0)

    def earliestTimeOf(batch: BatchID) = batch.id match {
      case 0L => new java.util.Date(Long.MinValue)
      case 1L => new java.util.Date(0)
      case 2L => new java.util.Date(Long.MaxValue)
    }
  }

  property("ScaldingPlatform matches Scala for single step jobs, with everything in one Batch") =
    forAll { (original: List[Int], fn: (Int) => List[(Int, Int)]) =>
      val inMemory = TestGraphs.singleStepInScala(original)(fn)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = simpleBatcher
      import Dsl._ // Won't be needed in scalding 0.9.0
      val testStore = new TestStore[Int,Int]("test", batcher, BatchID(0), Iterable.empty, BatchID(1))
      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding,(Long, Int),Int,Int](source, testStore) { tup => fn(tup._2) }

      val intr = Interval.leftClosedRightOpen(0L, original.size.toLong)
      val scald = new Scalding("scalaCheckJob",
        intr,
        TestMode(testStore.sourceToBuffer ++ buffer))

      scald.run(scald.plan(summer))
      // Now check that the inMemory ==

      val smap = testStore.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemory, smap)) == false
    }

  property("ScaldingPlatform matches Scala for leftJoin jobs, with everything in one Batch") =
    forAll { (original: List[Int],
      prejoinMap: (Int) => List[(Int, Int)],
      service: (Int,Int) => Option[Int],
      postJoin: ((Int, (Int, Option[Int]))) => List[(Int, Int)]) =>

      // We need to keep track of time correctly to use the service
      var fakeTime = -1
      val timeIncIt = new Iterator[Int] {
        val inner = original.iterator
        def hasNext = inner.hasNext
        def next = {
          fakeTime += 1
          inner.next
        }
      }
      val srvWithTime = { (key: Int) => service(fakeTime, key) }
      val inMemory = TestGraphs.leftJoinInScala(timeIncIt)(srvWithTime)(prejoinMap)(postJoin)

      // Add a time:
      val allKeys = original.flatMap(prejoinMap).map { _._1 }
      val allTimes = (0 until original.size)
      val stream = for { time <- allTimes; key <- allKeys; v = service(time, key) } yield (time.toLong, (key, v))

      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = simpleBatcher
      import Dsl._ // Won't be needed in scalding 0.9.0
      val testStore = new TestStore[Int,Int]("test", batcher, BatchID(0), Iterable.empty, BatchID(1))
      val testService = new TestService[Int, Int]("srv",
        batcher,
        Map(BatchID(0) -> Iterable.empty),
        Map(BatchID(1) -> stream))

      val (buffer, source) = testSource(inWithTime)

      val summer =
        TestGraphs.leftJoinJob[Scalding,(Long, Int),Int,Int,Int,Int](source, testService, testStore) { tup => prejoinMap(tup._2) }(postJoin)

      val intr = Interval.leftClosedRightOpen(0L, original.size.toLong)
      val scald = new Scalding("scalaCheckleftJoinJob",
        intr,
        TestMode(testStore.sourceToBuffer ++ buffer ++ testService.sourceToBuffer))

      scald.run(summer)
      // Now check that the inMemory ==

      val smap = testStore.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemory, smap)) == false
    }
}
