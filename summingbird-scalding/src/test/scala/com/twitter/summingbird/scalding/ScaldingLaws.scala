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
import com.twitter.summingbird._
import com.twitter.summingbird.monad._
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

class MockMappable[T](val id: String)(implicit tconv: TupleConverter[T]) extends ScaldingSource with Mappable[T] {
  val converter = tconv
  override def toString = id
  override def equals(that: Any) = that match {
    case m: MockMappable[_] => m.id == id
    case _ => false
  }
  override def hashCode = id.hashCode

  override def localScheme : LocalScheme = {
    // This is a hack because the MemoryTap doesn't actually care what the scheme is
    // it just holds the fields
    // TODO implement a proper Scheme for MemoryTap
    new CLTextDelimited(sourceFields, "\t", null : Array[Class[_]])
  }
}


class TestStore[K, V](store: String, inBatcher: Batcher, initBatch: BatchID, initStore: Iterable[(Time, (K, V))], lastBatch: BatchID)
(implicit ord: Ordering[K], tset: TupleSetter[(Time, (K, V))], tconv: TupleConverter[(Time, (K, V))])
  extends BatchedScaldingStore[K, V] {

  val batches: Map[BatchID, Mappable[(Time, (K, V))]] =
    BatchID.range(initBatch, lastBatch).map { b => (b, mockFor(b)) }.toMap

  // Needed to init the Test mode:
  val sourceToBuffer: Map[ScaldingSource, Buffer[Tuple]] =
    BatchID.range(initBatch, lastBatch).map { b =>
      if (initBatch == b) (batches(b), initStore.map { tset(_) }.toBuffer)
      else (batches(b), Buffer.empty[Tuple])
    }.toMap

  // Call this after you compute to check the results of the
  def lastToIterable(b: BatchID): Iterable[(Time, (K, V))] =
    sourceToBuffer(batches(b)).toIterable.map { tup => tconv(new TupleEntry(tup)) }

  val batcher = inBatcher
  val ordering = ord

  def mockFor(b: BatchID): Mappable[(Time, (K, V))] =
    new MockMappable(store + b.toString)

  override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowToPipe[(K, V)])] = {
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
  override def writeLast(batchID: BatchID, lastVals: KeyValuePipe[K, V])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    val out = batches(batchID)
    lastVals.write(out)
  }
}

object ScaldingLaws extends Properties("Scalding") {
  // TODO: These functions were lifted from Storehaus's testing
  // suite. They should move into Algebird to make it easier to test
  // maps that have had their zeros removed with MapAlgebra.

  def rightContainsLeft[K,V: Equiv](l: Map[K, V], r: Map[K, V]): Boolean =
    l.foldLeft(true) { (acc, pair) =>
      acc && r.get(pair._1).map { Equiv[V].equiv(_, pair._2) }.getOrElse(true)
    }

  implicit def mapEquiv[K,V: Monoid: Equiv]: Equiv[Map[K, V]] = {
    Equiv.fromFunction { (m1, m2) =>
      val cleanM1 = MapAlgebra.removeZeros(m1)
      val cleanM2 = MapAlgebra.removeZeros(m2)
      rightContainsLeft(cleanM1, cleanM2) && rightContainsLeft(cleanM2, cleanM1)
    }
  }

  def testSource[T](iter: Iterable[T])
    (implicit mf: Manifest[T], te: TimeExtractor[T], tc: TupleConverter[T], tset: TupleSetter[T]):
    (Map[ScaldingSource, Buffer[Tuple]], Producer[Scalding, T]) = {
      val src = IterableSource(iter)
      val prod = Scalding.sourceFromMappable { _ => src }
      (Map(src -> iter.map { tset(_) }.toBuffer), prod)
  }

  implicit def tupleExtractor[T <: (Long, _)]: TimeExtractor[T] = TimeExtractor( _._1 )

  property("ScaldingPlatform matches Scala for single step jobs, with everything in one Batch") =
    forAll { (original: List[Int], fn: (Int) => List[(Int, Int)]) =>
      val inMemory = TestGraphs.singleStepInScala(original)(fn)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = new Batcher {
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

      val smap = testStore.lastToIterable(BatchID(1)).map { _._2 }.toMap
      Monoid.isNonZero(Group.minus(inMemory, smap)) == false
    }
}
