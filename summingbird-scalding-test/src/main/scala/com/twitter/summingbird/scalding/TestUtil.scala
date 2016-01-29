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

import com.twitter.algebird.{ MapAlgebra, Monoid, Group, Interval, Last }
import com.twitter.algebird.monad._
import com.twitter.summingbird.batch._
import com.twitter.summingbird.TimeExtractor

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties

object TestUtil {
  def simpleTimeExtractor[T <: (Long, _)]: TimeExtractor[T] = TimeExtractor(_._1)

  def compareMaps[K, V: Group](original: Iterable[Any], inMemory: Map[K, V], produced: Map[K, V], name: String)(implicit batcher: Batcher): Boolean = {
    val diffMap = Group.minus(inMemory, produced)
    val wrong = Monoid.isNonZero(diffMap)
    if (wrong) {
      if (!name.isEmpty) println("%s is wrong".format(name))
      println("input: " + original)
      println("input size: " + original.size)
      println("input batches: " + batcher.batchOf(Timestamp(original.size)))
      println("producer extra keys: " + (produced.keySet -- inMemory.keySet))
      println("producer missing keys: " + (inMemory.keySet -- produced.keySet))
      println("Difference: " + diffMap)
    }
    !wrong
  }

  def compareMaps[K, V: Group](original: Iterable[Any], inMemory: Map[K, V], testStore: TestStore[K, V], name: String = ""): Boolean = {
    val produced = testStore.lastToIterable.toMap
    val diffMap = Group.minus(inMemory, produced)
    val wrong = Monoid.isNonZero(diffMap)
    if (wrong) {
      if (!name.isEmpty) println("%s is wrong".format(name))
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

  /**
   * Prunes the input to contain only values whose timestamps are covered completely
   * by the batches. This is used for the scala part of the job tests.
   * Keep both time and value.
   */
  def pruneToBatchCoveredWithTime[T](input: TraversableOnce[(Long, T)], inputRange: Interval[Timestamp], batcher: Batcher): TraversableOnce[(Long, T)] = {
    val batchRange = batcher.toTimestamp(batcher.batchesCoveredBy(inputRange))
    input.filter { case (ts, _) => batchRange.contains(Timestamp(ts)) }
  }

  /* keep just the values */
  def pruneToBatchCovered[T](input: TraversableOnce[(Long, T)], inputRange: Interval[Timestamp], batcher: Batcher): TraversableOnce[T] = {
    pruneToBatchCoveredWithTime(input, inputRange, batcher).map { case (ts, v) => v }
  }

  /**
   * This converts the min and max times to a time interval.
   * maxTime is an exclusive upper bound.
   */
  def toTimeInterval(minTime: Long, maxTime: Long): Interval[Timestamp] =
    Interval.leftClosedRightOpen(Timestamp(minTime), Timestamp(maxTime))

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
    if (items.isEmpty) simpleBatcher
    else randomBatcher(items.iterator.map(_._1).min, items.iterator.map(_._1).max)
  }

  def randomBatcher(mintimeInc: Long, maxtimeInc: Long): Batcher = { //simpleBatcher
    // we can have between 1 and (maxtime - mintime + 1) batches.
    val delta = (maxtimeInc - mintimeInc)
    val MaxBatches = 5L min delta
    val batches = 1L + Gen.choose(0L, MaxBatches).sample.get
    val timePerBatch = (delta + 1L) / batches
    new MillisecondBatcher(timePerBatch)
  }
}
