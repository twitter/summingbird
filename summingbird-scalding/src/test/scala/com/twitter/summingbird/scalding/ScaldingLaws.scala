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

import com.twitter.algebird.{MapAlgebra, Monoid, Group, Interval}
import com.twitter.algebird.monad._
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher, Timestamp}

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }
import com.twitter.scalding.typed.TypedSink

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap => MutableHashMap, Map => MutableMap, SynchronizedBuffer, SynchronizedMap}

import cascading.scheme.local.{TextDelimited => CLTextDelimited}
import cascading.tuple.{Tuple, Fields, TupleEntry}
import cascading.flow.FlowDef
import cascading.tap.Tap
import cascading.scheme.NullScheme
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector


import org.specs._

/**
  * Tests for Summingbird's Scalding planner.
  */

class MockMappable[T](val id: String)(implicit tconv: TupleConverter[T])
    extends ScaldingSource with Mappable[T] {
  def converter[U >: T] = TupleConverter.asSuperConverter(tconv)
  override def toString = id
  override def equals(that: Any) = that match {
    case m: MockMappable[_] => m.id == id
    case _ => false
  }
  override def hashCode = id.hashCode

  override def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] =
    TestTapFactory(this, new NullScheme[JobConf, RecordReader[_,_], OutputCollector[_,_], T, T](Fields.ALL, Fields.ALL)).createTap(readOrWrite)
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
      val rdr = Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable)(fd._1, fd._2) }
      Right((batch, rdr))
    }
  }
  /** Instances may choose to write out the last or just compute it from the stream */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    val out = batches(batchID)
    lastVals.write(TypedSink[(K, V)](out))
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
      Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable)(fd._1, fd._2) }
    }
  }
  override def readLast(exclusiveUB: BatchID, mode: Mode) = {
    val candidates = lasts.filter { _._1 < exclusiveUB }
    if(candidates.isEmpty) {
      Left(List("No batches < :" + exclusiveUB.toString))
    }
    else {
      val (batch, _) = candidates.maxBy { _._1 }
      val mappable = lastMappable(batch)
      val rdr = Reader { (fd: (FlowDef, Mode)) =>
        TypedPipe.from(mappable)(fd._1, fd._2).values
      }
      Right((batch, rdr))
    }
  }
}

/** This is a test sink that assumes single threaded testing with
 * cascading local mode
 */
class TestSink[T] extends ScaldingSink[T] {
  private var data: Vector[(Long, T)] = Vector.empty

  def write(incoming: PipeFactory[T]): PipeFactory[T] =
    // three functors deep:
    incoming.map { state =>
      state.map { reader =>
        reader.map { timeItem =>
          data = data :+ timeItem
          timeItem
        }
      }
    }

  def reset: Vector[(Long, T)] = {
    val oldData = data
    data = Vector.empty
    oldData
  }
}

// This is not really usable, just a mock that does the same state over and over
class LoopState[T](init: Interval[T]) extends WaitingState[T] { self =>
  def begin = new RunningState[T] {
    def part = self.init
    def succeed(nextStart: Interval[T]) = self
    def fail(err: Throwable) = {
      println(err)
      self
    }
  }
}

object ScaldingLaws extends Specification {
  import MapAlgebra.sparseEquiv

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def testSource[T](iter: Iterable[T])
    (implicit mf: Manifest[T], te: TimeExtractor[T], tc: TupleConverter[T], tset: TupleSetter[T]):
    (Map[ScaldingSource, Buffer[Tuple]], Producer[Scalding, T]) = {
      val src = IterableSource(iter)
      val prod = Scalding.sourceFromMappable { _ => src }
      (Map(src -> iter.map { tset(_) }.toBuffer), prod)
  }

  implicit def tupleExtractor[T <: (Long, _)]: TimeExtractor[T] = TimeExtractor( _._1 )

  val simpleBatcher = new Batcher {
    def batchOf(d: Timestamp) =
      if (d.milliSinceEpoch == Long.MaxValue) BatchID(2)
      else if (d.milliSinceEpoch >= 0L) BatchID(1)
      else BatchID(0)

    def earliestTimeOf(batch: BatchID) = batch.id match {
      case 0L => Timestamp.Min
      case 1L => Timestamp(0)
      case 2L => Timestamp.Max
    }
  }

  "The ScaldingPlatform" should {
    //Set up the job:
    "match scala for single step jobs" in {
      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]
      val inMemory = TestGraphs.singleStepInScala(original)(fn)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = simpleBatcher
      import Dsl._ // Won't be needed in scalding 0.9.0
      val testStore = new TestStore[Int,Int]("test", batcher, BatchID(0), Iterable.empty, BatchID(1))
      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding,(Long,Int),Int,Int](source, testStore)(t =>
          fn(t._2))

      val intr = Interval.leftClosedRightOpen(0L, original.size.toLong)
      val scald = new Scalding("scalaCheckJob")
      val ws = new LoopState(intr.mapNonDecreasing(t => Timestamp(t)))
      val mode: Mode = TestMode(t => (testStore.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(summer))
      // Now check that the inMemory ==

      val smap = testStore.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemory, smap)) must be(false)
    }
    
    "match scala for flatMapKeys jobs" in {
      val original = sample[List[Int]]
      val fnA = sample[(Int) => List[(Int, Int)]]
      val fnB = sample[Int => List[Int]]
      val inMemory = TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = simpleBatcher
      import Dsl._ // Won't be needed in scalding 0.9.0
      val testStore = new TestStore[Int,Int]("test", batcher, BatchID(0), Iterable.empty, BatchID(1))
      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs.singleStepMapKeysJob[Scalding,(Long,Int),Int,Int, Int](source, testStore)(t =>
          fnA(t._2), fnB)

      val intr = Interval.leftClosedRightOpen(0L, original.size.toLong)
      val scald = new Scalding("scalaCheckJob")
      val ws = new LoopState(intr.mapNonDecreasing(t => Timestamp(t)))
      val mode: Mode = TestMode(t => (testStore.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(summer))
      // Now check that the inMemory ==

      val smap = testStore.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemory, smap)) must be(false)
    }
    


    "match scala for multiple summer jobs" in {
      val original = sample[List[Int]]
      val fnA = sample[(Int) => List[(Int)]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val fnC = sample[(Int) => List[(Int, Int)]]
      val (inMemoryA, inMemoryB) = TestGraphs.multipleSummerJobInScala(original)(fnA, fnB, fnC)

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = simpleBatcher
      import Dsl._ // Won't be needed in scalding 0.9.0
      val testStoreA = new TestStore[Int,Int]("testA", batcher, BatchID(0), Iterable.empty, BatchID(1))
      val testStoreB = new TestStore[Int,Int]("testB", batcher, BatchID(0), Iterable.empty, BatchID(1))
      val (buffer, source) = testSource(inWithTime)

      val tail = TestGraphs.multipleSummerJob[Scalding, (Long, Int), Int, Int, Int, Int, Int](source, testStoreA, testStoreB)({t => fnA(t._2)}, fnB, fnC)

      val intr = Interval.leftClosedRightOpen(0L, original.size.toLong)
      val scald = new Scalding("scalaCheckMultipleSumJob")
      val ws = new LoopState(intr.mapNonDecreasing(t => Timestamp(t)))
      val mode: Mode = TestMode(t => (testStoreA.sourceToBuffer ++ testStoreB.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(tail))
      // Now check that the inMemory ==

      val smapA = testStoreA.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemoryA, smapA)) must be(false)

      val smapB = testStoreB.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemoryB, smapB)) must be(false)
    }


    "match scala for leftJoin jobs" in {
      val original = sample[List[Int]]
      val prejoinMap = sample[(Int) => List[(Int, Int)]]
      val service = sample[(Int,Int) => Option[Int]]
      val postJoin = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]
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
      val scald = new Scalding("scalaCheckleftJoinJob")
      val ws = new LoopState(intr.mapNonDecreasing(t => Timestamp(t)))
      val mode: Mode = TestMode(s => (testStore.sourceToBuffer ++ buffer ++ testService.sourceToBuffer).get(s))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      val smap = testStore.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemory, smap)) must be (false)
    }

    "match scala for diamond jobs with write & everything in one Batch" in {
      val original = sample[List[Int]]
      val fn1 = sample[(Int) => List[(Int, Int)]]
      val fn2 = sample[(Int) => List[(Int, Int)]]
      val inMemory = TestGraphs.diamondJobInScala(original)(fn1)(fn2)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = simpleBatcher
      import Dsl._ // Won't be needed in scalding 0.9.0
      val testStore = new TestStore[Int,Int]("test", batcher, BatchID(0),
        Iterable.empty, BatchID(1))
      val testSink = new TestSink[(Long,Int)]
      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs
        .diamondJob[Scalding,(Long, Int),Int,Int](source,
          testSink,
          testStore)(t => fn1(t._2))(t => fn2(t._2))

      val scald = new Scalding("scalding-diamond-Job")
      val intr = Interval.leftClosedRightOpen(0L, original.size.toLong)
      val ws = new LoopState(intr.mapNonDecreasing(t => Timestamp(t)))
      val mode: Mode = TestMode(s => (testStore.sourceToBuffer ++ buffer).get(s))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

        val sinkOut = testSink.reset
        println(sinkOut)
        println(inWithTime)
      val smap = testStore.lastToIterable(BatchID(1)).toMap
      Monoid.isNonZero(Group.minus(inMemory, smap)) must be (false)
      
      sinkOut.map { _._2 }.toList must be_== (inWithTime)
    }
  }
}

object VersionBatchLaws extends Properties("VersionBatchLaws") {
  property("version -> BatchID -> version") = forAll { (l: Long) =>
    val vbs = new VersionedBatchStore[Int, Int, Array[Byte], Array[Byte]](null,
      0, Batcher.ofHours(1))(null)(null)
    val b = vbs.versionToBatchID(l)
    vbs.batchIDToVersion(b) <= l
  }
  property("BatchID -> version -> BatchID") = forAll { (bint: Int) =>
    val b = BatchID(bint)
    val vbs = new VersionedBatchStore[Int, Int, Array[Byte], Array[Byte]](null,
      0, Batcher.ofHours(1))(null)(null)
    val v = vbs.batchIDToVersion(b)
    vbs.versionToBatchID(v) == b
  }
  property("version is an upperbound on time") = forAll { (lBig: Long) =>
    val l = lBig/1000L
    val batcher = Batcher.ofHours(1)
    val vbs = new VersionedBatchStore[Int, Int, Array[Byte], Array[Byte]](null,
      0, batcher)(null)(null)
    val b = vbs.versionToBatchID(l)
    (batcher.earliestTimeOf(b.next).milliSinceEpoch <= l) &&
    (batcher.earliestTimeOf(b).milliSinceEpoch < l)
    (batcher.earliestTimeOf(b.next.next).milliSinceEpoch > l)
  }
}

class ScaldingSerializationSpecs extends Specification {
  noDetailedDiffs()

  implicit def tupleExtractor[T <: (Long, _)]: TimeExtractor[T] = TimeExtractor( _._1 )

  "ScaldingPlatform" should {
    "serialize Hadoop Jobs for single step jobs" in {
      // Add a time:
      val inWithTime = List(1, 2, 3).zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = ScaldingLaws.simpleBatcher
      import Dsl._ // Won't be needed in scalding 0.9.0
      val testStore = new TestStore[Int,Int]("test", batcher, BatchID(0), Iterable.empty, BatchID(1))
      val (buffer, source) = ScaldingLaws.testSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding,(Long, Int),Int,Int](source, testStore) {
        tup => List((1 -> tup._2))
      }

      val mode = HadoopTest(new Configuration, {case x: ScaldingSource => buffer.get(x)})
      val intr = Interval.leftClosedRightOpen(0L, inWithTime.size.toLong)
      val scald = new Scalding("scalaCheckJob")

      (try { scald.toFlow(intr, mode, scald.plan(summer)); true }
      catch { case t: Throwable => println(toTry(t)); false }) must beTrue
    }
  }
}
