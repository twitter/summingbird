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

package com.twitter.summingbird.batch

import com.twitter.algebird.Monoid
import com.twitter.algebird.{
  Empty,
  Interval,
  Intersection,
  InclusiveLower,
  ExclusiveUpper,
  InclusiveUpper,
  ExclusiveLower,
  Universe
}
import com.twitter.bijection.{ Bijection, Injection }
import scala.collection.Iterator.iterate

/**
 * The Batch is the fundamental work unit of the Hadoop portion of
 * Summingbird. Batches are processed offline and pushed into a
 * persistent store for serving. The offline Batches include the sum
 * of all Values for each Key. If the Value is zero, it is omitted
 * (i.e. zero[Value] is indistinguishable from having never seen a
 * Value for a given Key).  Each Batch has a unique BatchID. Each
 * event falls into a single BatchID (which is a concrete type
 * isomorphic to Long).
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object BatchID {
  import OrderedFromOrderingExt._
  implicit val equiv: Equiv[BatchID] = Equiv.by(_.id)

  // Enables BatchID(someBatchID.toString) roundtripping
  def apply(str: String) = new BatchID(str.split("\\.")(1).toLong)

  /**
   * Returns an Iterator[BatchID] containing the range
   * `[startBatch, endBatch]` (inclusive).
   */
  def range(start: BatchID, end: BatchID): Iterable[BatchID] =
    new Iterable[BatchID] {
      def iterator = iterate(start)(_.next).takeWhile(_ <= end)
    }

  /**
   * Returns true if the supplied interval of BatchID can
   */
  def toInterval(iter: TraversableOnce[BatchID]): Option[Interval[BatchID]] =
    iter
      .map { b => (b, b, 1L) }
      .reduceOption { (left, right) =>
        val (lmin, lmax, lcnt) = left
        val (rmin, rmax, rcnt) = right
        (lmin min rmin, lmax max rmax, lcnt + rcnt)
      }
      .flatMap {
        case (min, max, cnt) =>
          if ((min + cnt) == (max + 1L)) {
            Some(Interval.leftClosedRightOpen(min, max.next).right.get)
          } else {
            // These batches are not contiguous, not an interval
            None
          }
      }
      .orElse(Some(Empty[BatchID]())) // there was nothing it iter

  /**
   * Returns all the BatchIDs that are contained in the interval
   */
  def toIterable(interval: Interval[BatchID]): Iterable[BatchID] =
    interval match {
      case Empty() => Iterable.empty
      case Universe() => range(Min, Max)
      case ExclusiveUpper(upper) => range(Min, upper.prev)
      case InclusiveUpper(upper) => range(Min, upper)
      case ExclusiveLower(lower) => range(lower.next, Max)
      case InclusiveLower(lower) => range(lower, Max)
      case Intersection(low, high) =>
        val lowbatch = low match {
          case InclusiveLower(lb) => lb
          case ExclusiveLower(lb) => lb.next
        }
        val highbatch = high match {
          case InclusiveUpper(hb) => hb
          case ExclusiveUpper(hb) => hb.prev
        }
        range(lowbatch, highbatch)
    }

  val Max = BatchID(Long.MaxValue)
  val Min = BatchID(Long.MinValue)

  implicit val monoid: Monoid[BatchID] = new Monoid[BatchID] {
    override val zero = BatchID(Long.MinValue)
    override def plus(l: BatchID, r: BatchID) = if (l >= r) l else r
  }

  implicit val batchID2String: Injection[BatchID, String] =
    Injection.buildCatchInvert[BatchID, String] { _.toString } { BatchID(_) }

  implicit val batchID2Long: Bijection[BatchID, Long] =
    Bijection.build[BatchID, Long] { _.id } { BatchID(_) }

  implicit val batchID2Bytes: Injection[BatchID, Array[Byte]] =
    Injection.connect[BatchID, Long, Array[Byte]]

  implicit val batchIdOrdering: Ordering[BatchID] = Ordering.by(_.id)
}

case class BatchID(id: Long) extends AnyVal {
  import OrderedFromOrderingExt._
  def next: BatchID = new BatchID(id + 1)
  def prev: BatchID = new BatchID(id - 1)
  def +(cnt: Long) = new BatchID(id + cnt)

  def -(cnt: Long) = new BatchID(id - cnt)
  def max(b: BatchID) = if (this >= b) this else b
  def min(b: BatchID) = if (this < b) this else b

  override def toString = "BatchID." + id.toString
}
