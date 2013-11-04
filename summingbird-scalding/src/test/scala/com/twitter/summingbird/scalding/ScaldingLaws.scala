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

import com.twitter.algebird.{MapAlgebra, Monoid, Group, Interval, Last}
import com.twitter.algebird.monad._
import com.twitter.summingbird._
import com.twitter.summingbird.batch._
import com.twitter.summingbird.scalding.state.HDFSState

import java.util.TimeZone
import java.io.File

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


import org.specs2.mutable._

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

object TestStore {
  def apply[K, V](store: String, inBatcher: Batcher, initStore: Iterable[(K, V)], lastTime: Long)
    (implicit ord: Ordering[K], tset: TupleSetter[(K, V)], tconv: TupleConverter[(K, V)]) = {
    val startBatch = inBatcher.batchOf(Timestamp(0)).prev
    val endBatch = inBatcher.batchOf(Timestamp(lastTime)).next
    new TestStore[K, V](store, inBatcher, startBatch, initStore, endBatch)
  }
}

class TestStore[K, V](store: String, inBatcher: Batcher, initBatch: BatchID, initStore: Iterable[(K, V)], lastBatch: BatchID)
(implicit ord: Ordering[K], tset: TupleSetter[(K, V)], tconv: TupleConverter[(K, V)])
  extends BatchedScaldingStore[K, V] {

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
    if(candidates.isEmpty) {
      Left(List("No batches < :" + exclusiveUB.toString))
    }
    else {
      val batch = candidates.max
      val mappable = batches(batch)
      val rdr = Reader { (fd: (FlowDef, Mode)) => TypedPipe.from(mappable)(fd._1, fd._2) }
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

class TestService[K, V](service: String,
  inBatcher: Batcher,
  minBatch: BatchID,
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

  /** The lasts are computed from the streams */
  lazy val lasts: Map[BatchID, Iterable[(Time, (K, V))]] = {
    (streams
      .toList
      .sortBy(_._1)
      .foldLeft(Map.empty[BatchID, Map[K, (Time, V)]]) {
        case (map, (batch: BatchID, writes: Iterable[(Time, (K, Option[V]))])) =>
          val thisBatch = writes.foldLeft(map.get(batch).getOrElse(Map.empty[K, (Time, V)])) {
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
class LoopState[T](init: T) extends WaitingState[T] { self =>
  def begin = new PrepareState[T] {
    def requested = self.init
    def fail(err: Throwable) = {
      println(err)
      self
    }
    def willAccept(intr: T) = Right(new RunningState[T] {
      def succeed = self
      def fail(err: Throwable) = {
        println(err)
        self
      }
    })
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

  def compareMaps[K,V:Group](original: Iterable[Any], inMemory: Map[K, V], testStore: TestStore[K, V], name: String = ""): Boolean = {
    val produced = testStore.lastToIterable.toMap
    val diffMap = Group.minus(inMemory, produced)
    val wrong = Monoid.isNonZero(diffMap)
    if(wrong) {
      if(!name.isEmpty) println("%s is wrong".format(name))
      println("input: " + original)
      println("input size: " + original.size)
      println("input batches: " + testStore.batcher.batchOf(Timestamp(original.size)))
      println("producer extra keys: " + (produced.keySet -- inMemory.keySet))
      println("producer missing keys: " + (inMemory.keySet -- produced.keySet))
      println("written batches: " + testStore.writtenBatches)
      println("earliest unwritten time: " + testStore.batcher.earliestTimeOf(testStore.writtenBatches.max.next))
      println("Difference: " + diffMap)
    }
    !wrong
  }

  def batchedCover(batcher: Batcher, minTime: Long, maxTime: Long): Interval[Timestamp] =
    batcher.cover(
      Interval.leftClosedRightOpen(Timestamp(minTime), Timestamp(maxTime+1L))
    ).mapNonDecreasing(b => batcher.earliestTimeOf(b.next))

  val simpleBatcher = new Batcher {
    def batchOf(d: Timestamp) =
      if (d == Timestamp.Max) BatchID(2)
      else if (d.milliSinceEpoch >= 0L) BatchID(1)
      else BatchID(0)

    def earliestTimeOf(batch: BatchID) = batch.id match {
      case 0L => Timestamp.Min
      case 1L => Timestamp(0)
      case 2L => Timestamp.Max
      case 3L => Timestamp.Max
    }
    // this is just for testing, it covers everything with batch 1
    override def cover(interval: Interval[Timestamp]): Interval[BatchID] =
      Interval.leftClosedRightOpen(BatchID(1), BatchID(2))
  }

  def randomBatcher(items: Iterable[(Long, Any)]): Batcher = {
    if(items.isEmpty) simpleBatcher
    else randomBatcher(items.iterator.map(_._1).min, items.iterator.map(_._1).max)
  }

  def randomBatcher(mintimeInc: Long, maxtimeInc: Long): Batcher = { //simpleBatcher
    // we can have between 1 and (maxtime - mintime + 1) batches.
    val delta = (maxtimeInc - mintimeInc)
    val MaxBatches = 5L min delta
    val batches = 1L + Gen.choose(0L, MaxBatches).sample.get
    if(batches == 1L) simpleBatcher
    else {
      val timePerBatch = (delta + 1L)/batches
      new MillisecondBatcher(timePerBatch)
    }
  }

  "The ScaldingPlatform" should {
    //Set up the job:
    "match scala for single step jobs" in {
      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]
      val inMemory = TestGraphs.singleStepInScala(original)(fn)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = randomBatcher(inWithTime)
      val testStore = TestStore[Int,Int]("test", batcher, initStore, inWithTime.size)
      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding,(Long,Int),Int,Int](source, testStore)(t =>
          fn(t._2))

      val scald = Scalding("scalaCheckJob")
      val intr = batchedCover(batcher, 0L, original.size.toLong)
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(t => (testStore.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(summer))
      // Now check that the inMemory ==

      compareMaps(original, Monoid.plus(initStore, inMemory), testStore) must be_==(true)
    }

    "match scala for flatMapKeys jobs" in {
      val original = sample[List[Int]]
      val initStore = sample[Map[Int,Int]]
      val fnA = sample[(Int) => List[(Int, Int)]]
      val fnB = sample[Int => List[Int]]
      val inMemory = TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = randomBatcher(inWithTime)
      val testStore = TestStore[Int,Int]("test", batcher, initStore, inWithTime.size)

      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs.singleStepMapKeysJob[Scalding,(Long,Int),Int,Int, Int](source, testStore)(t =>
          fnA(t._2), fnB)

      val intr = batchedCover(batcher, 0L, original.size.toLong)
      val scald = Scalding("scalaCheckJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(t => (testStore.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(summer))
      // Now check that the inMemory ==

      compareMaps(original, Monoid.plus(initStore, inMemory), testStore) must beTrue
    }

    "match scala for multiple summer jobs" in {
      val original = sample[List[Int]]
      val initStoreA = sample[Map[Int,Int]]
      val initStoreB = sample[Map[Int,Int]]
      val fnA = sample[(Int) => List[(Int)]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val fnC = sample[(Int) => List[(Int, Int)]]
      val (inMemoryA, inMemoryB) = TestGraphs.multipleSummerJobInScala(original)(fnA, fnB, fnC)

      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = randomBatcher(inWithTime)
      val testStoreA = TestStore[Int,Int]("testA", batcher, initStoreA, inWithTime.size)
      val testStoreB = TestStore[Int,Int]("testB", batcher, initStoreB, inWithTime.size)
      val (buffer, source) = testSource(inWithTime)

      val tail = TestGraphs.multipleSummerJob[Scalding, (Long, Int), Int, Int, Int, Int, Int](source, testStoreA, testStoreB)({t => fnA(t._2)}, fnB, fnC)

      val scald = Scalding("scalaCheckMultipleSumJob")
      val intr = batchedCover(batcher, 0L, original.size.toLong)
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(t => (testStoreA.sourceToBuffer ++ testStoreB.sourceToBuffer ++ buffer).get(t))

      scald.run(ws, mode, scald.plan(tail))
      // Now check that the inMemory ==

      compareMaps(original, Monoid.plus(initStoreA, inMemoryA), testStoreA) must beTrue
      compareMaps(original, Monoid.plus(initStoreB, inMemoryB), testStoreB) must beTrue
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
      val batcher = randomBatcher(inWithTime)
      val initStore = sample[Map[Int, Int]]
      val testStore = TestStore[Int,Int]("test", batcher, initStore, inWithTime.size)

      /**
       * Create the batched service
       */
      val batchedService = stream.groupBy { case (time, _) => batcher.batchOf(Timestamp(time)) }
      val testService = new TestService[Int, Int]("srv", batcher, batcher.batchOf(Timestamp(0)).prev, batchedService)

      val (buffer, source) = testSource(inWithTime)

      val summer =
        TestGraphs.leftJoinJob[Scalding,(Long, Int),Int,Int,Int,Int](source, testService, testStore) { tup => prejoinMap(tup._2) }(postJoin)

      val intr = batchedCover(batcher, 0L, original.size.toLong)
      val scald = Scalding("scalaCheckleftJoinJob")
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(s => (testStore.sourceToBuffer ++ buffer ++ testService.sourceToBuffer).get(s))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      compareMaps(original, Monoid.plus(initStore, inMemory), testStore) must beTrue
    }

    "match scala for diamond jobs with write" in {
      val original = sample[List[Int]]
      val fn1 = sample[(Int) => List[(Int, Int)]]
      val fn2 = sample[(Int) => List[(Int, Int)]]
      val inMemory = TestGraphs.diamondJobInScala(original)(fn1)(fn2)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = randomBatcher(inWithTime)
      val initStore = sample[Map[Int, Int]]
      val testStore = TestStore[Int,Int]("test", batcher, initStore, inWithTime.size)
      val testSink = new TestSink[(Long,Int)]
      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs
        .diamondJob[Scalding,(Long, Int),Int,Int](source,
          testSink,
          testStore)(t => fn1(t._2))(t => fn2(t._2))

      val scald = Scalding("scalding-diamond-Job")
      val intr = batchedCover(batcher, 0L, original.size.toLong)
      val ws = new LoopState(intr)
      val mode: Mode = TestMode(s => (testStore.sourceToBuffer ++ buffer).get(s))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      val sinkOut = testSink.reset
      compareMaps(original, Monoid.plus(initStore, inMemory), testStore) must beTrue
      val wrongSink = sinkOut.map { _._2 }.toList != inWithTime
      wrongSink must be_==(false)
      if(wrongSink) {
        println("input: " + inWithTime)
        println("SinkExtra: " + (sinkOut.map(_._2).toSet -- inWithTime.toSet))
        println("SinkMissing: " + (inWithTime.toSet -- sinkOut.map(_._2).toSet))
      }
    }

    "Correctly aggregate multiple sumByKeys" in {
      val original = sample[List[(Int,Int)]]
      val keyExpand = sample[(Int) => List[Int]]
      val (inMemoryA, inMemoryB) = TestGraphs.twoSumByKeyInScala(original, keyExpand)
      // Add a time:
      val inWithTime = original.zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = randomBatcher(inWithTime)
      val initStore = sample[Map[Int, Int]]
      val testStoreA = TestStore[Int,Int]("testA", batcher, initStore, inWithTime.size)
      val testStoreB = TestStore[Int,Int]("testB", batcher, initStore, inWithTime.size)
      val (buffer, source) = testSource(inWithTime)

      val summer = TestGraphs
        .twoSumByKey[Scalding,Int,Int,Int](source.map(_._2), testStoreA, keyExpand, testStoreB)

      val scald = Scalding("scalding-diamond-Job")
      val intr = batchedCover(batcher, 0L, original.size.toLong)
      val ws = new LoopState(intr)
      val mode: Mode = TestMode((testStoreA.sourceToBuffer ++ testStoreB.sourceToBuffer ++ buffer).get(_))

      scald.run(ws, mode, summer)
      // Now check that the inMemory ==

      compareMaps(original, Monoid.plus(initStore, inMemoryA), testStoreA, "A") must beTrue
      compareMaps(original, Monoid.plus(initStore, inMemoryB), testStoreB, "B") must beTrue
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


  implicit def tupleExtractor[T <: (Long, _)]: TimeExtractor[T] = TimeExtractor( _._1 )

  "ScaldingPlatform" should {
    "serialize Hadoop Jobs for single step jobs" in {
      // Add a time:
      val inWithTime = List(1, 2, 3).zipWithIndex.map { case (item, time) => (time.toLong, item) }
      val batcher = ScaldingLaws.randomBatcher(inWithTime)
      val testStore = TestStore[Int,Int]("test", batcher, Iterable.empty, inWithTime.size)
      val (buffer, source) = ScaldingLaws.testSource(inWithTime)

      val summer = TestGraphs.singleStepJob[Scalding,(Long, Int),Int,Int](source, testStore) {
        tup => List((1 -> tup._2))
      }

      val mode = HadoopTest(new Configuration, {case x: ScaldingSource => buffer.get(x)})
      val intr = Interval.leftClosedRightOpen(0L, inWithTime.size.toLong)
      val scald = Scalding("scalaCheckJob")

      (try { scald.toFlow(intr, mode, scald.plan(summer)); true }
      catch { case t: Throwable => println(toTry(t)); false }) must beTrue
    }
  }
}
