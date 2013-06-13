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
  def apply(long: Long) = new BatchID(long)
  // Enables BatchID(someBatchID.toString) roundtripping
  def apply(str: String) = new BatchID(str.split("\\.")(1).toLong)

  /**
   * Returns an Iterator[BatchID] containing the range
   * `[startBatch, endBatch]` (inclusive).
   */
  def range(start: BatchID, end: BatchID): Iterator[BatchID] =
    iterate(start)(_.next).takeWhile(_ <= end)

  val Max = BatchID(Long.MaxValue)
  val Min = BatchID(Long.MinValue)

  implicit val monoid: Monoid[BatchID] = new Monoid[BatchID] {
    override val zero = BatchID(Long.MinValue)
    override def plus(l: BatchID, r: BatchID) = if (l >= r) l else r
  }

  implicit val batchID2String: Injection[BatchID, String] =
    Injection.buildCatchInvert[BatchID,String] { _.toString } { BatchID(_) }

  implicit val batchID2Long: Bijection[BatchID, Long] =
    Bijection.build[BatchID,Long] { _.id } { BatchID(_) }

  implicit val batchID2Bytes: Injection[BatchID, Array[Byte]] =
    Injection.connect[BatchID, Long, Array[Byte]]
}

class BatchID(val id: Long) extends Ordered[BatchID] with java.io.Serializable {
  def next: BatchID = new BatchID(id + 1)
  def prev: BatchID = new BatchID(id - 1)
  def +(cnt: Long) = new BatchID(id + cnt)
  def -(cnt: Long) = new BatchID(id - cnt)
  def compare(b: BatchID) = id.compareTo(b.id)
  def max(b: BatchID) = if (this >= b) this else b
  override lazy val toString = "BatchID." + id.toString

  override def hashCode: Int = id.hashCode
  override def equals(other: Any): Boolean =
    other match {
      case that: BatchID => this.id == that.id
      case _ => false
    }
}
