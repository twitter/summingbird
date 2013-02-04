package com.twitter.summingbird.batch

import com.twitter.bijection.Bijection
import com.twitter.algebird.Monoid
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
 *  @author Oscar Boykin
 *  @author Sam Ritchie
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

  implicit val monoid: Monoid[BatchID] = new Monoid[BatchID] {
    override val zero = BatchID(Long.MinValue)
    override def plus(l: BatchID, r: BatchID) = if (l >= r) l else r
  }

  implicit val batchID2String: Bijection[BatchID, String] =
    Bijection.build[BatchID,String] { _.toString } { BatchID(_) }

  implicit val batchID2Long: Bijection[BatchID, Long] =
    Bijection.build[BatchID,Long] { _.id } { BatchID(_) }

  implicit val batchID2Bytes: Bijection[BatchID, Array[Byte]] =
    Bijection.connect[BatchID, Long, Array[Byte]]
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
