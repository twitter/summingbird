package com.twitter.summingbird.batch

import java.util.{ Comparator, TimeZone }
import com.twitter.bijection.Bijection
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ RichDate, DateOps, AbsoluteDuration }
import com.twitter.summingbird.bijection.BijectionImplicits._
import scala.collection.Iterator.iterate


/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// The Batch is the fundamental work unit of the Hadoop portion of
// Summingbird. Batches are processed offline and pushed into a
// persistent store for serving. The offline Batches include the sum
// of all Values for each Key. If the Value is zero, it is omitted
// (i.e. zero[Value] is indistinguishable from having never seen a
// Value for a given Key).  Each Batch has a unique BatchID. Each
// event falls into a single BatchID (which is a concrete type
// isomorphic to Long).

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


// For the purposes of batching, each Event object has exactly one
// Time. The Batcher[Time] uses this time to assign each Event to a
// specific BatchID. Batcher[Time] also has a Comparator[Time] so that
// Time is ordered. Lastly, a Batcher[Time] can return the minimum
// value of Time for each BatchID.  The Time type in practice might be
// a java Date, a Long representing millis since the epoch, or an Int
// representing seconds since the epoch, for instance.

trait Batcher[Time] extends java.io.Serializable {
  //  parseTime is used in Offline mode when the arguments to the job
  //  are strings which must be parsed. We need to have a way to convert
  //  from String to the Time type.
  def parseTime(s: String): Time
  def earliestTimeOf(batch: BatchID): Time
  def currentTime: Time
  def batchOf(t: Time): BatchID
  def timeComparator: Comparator[Time]

  def currentBatch = batchOf(currentTime)
}

abstract class DurationBatcher[Time](duration : AbsoluteDuration) extends Batcher[Time] {
  def timeToMillis(t : Time) : Long
  def millisToTime(millis : Long) : Time

  lazy val tz = TimeZone.getTimeZone("UTC")
  val durationMillis = duration.toMillisecs

  // The DurationBatcher requires strings formatted according ISO8601:
  // http://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations

  def parseTime(s: String) = millisToTime(DateOps.stringToRichDate(s)(tz).timestamp)
  def earliestTimeOf(batch: BatchID) = {
    val id = batch.id
    if(id >= 0L)
      millisToTime(id * durationMillis)
    else
      //Again deal with the rounding towards zero issue:
      millisToTime(id * durationMillis + 1L)
  }

  def batchOf(t : Time) = {
    val tInMillis = timeToMillis(t)
    val batch = BatchID(timeToMillis(t) / durationMillis)
    if (tInMillis < 0L) {
      // Due to the way rounding to zero, rather than -Inf,
      // we need to subtract a batch if t < 0
      batch.prev
    }
    else {
      batch
    }
  }

  lazy val timeComparator = new Comparator[Time] {
    def compare(l : Time, r : Time) = timeToMillis(l).compareTo(timeToMillis(r))
  }
}

// Use this class with a Time type of Long when the longs represent milliseconds.
class MillisecondsDurationBatcher(duration: AbsoluteDuration) extends DurationBatcher[Long](duration) {
  def timeToMillis(t : Long) = t
  def millisToTime(ms : Long) = ms
  def currentTime = System.currentTimeMillis
}

// Use this class with a Time type of Int when the ints represent seconds.
class SecondsDurationBatcher(duration: AbsoluteDuration) extends DurationBatcher[Int](duration) {
  def timeToMillis(t : Int) = t * 1000L
  def millisToTime(ms : Long) = (ms / 1000L).toInt
  def currentTime = (System.currentTimeMillis / 1000) toInt
}

// Batcher with a single, infinitely long batch. This makes sense for
// online-only jobs with a single store that will never need to merge
// at the batch level.
//
// We need to pass a value for a lower bound of any Time expected to
// be seen. The time doesn't matter, we just need some instance.

object UnitBatcher {
  def apply[Time: Ordering](currentTime: Time): UnitBatcher[Time] = new UnitBatcher(currentTime)
}
class UnitBatcher[Time: Ordering](override val currentTime: Time) extends Batcher[Time] {
  def parseTime(s: String) = currentTime
  def earliestTimeOf(batch: BatchID) = currentTime
  def batchOf(t: Time) = BatchID(0)
  lazy val timeComparator = implicitly[Ordering[Time]]
}
