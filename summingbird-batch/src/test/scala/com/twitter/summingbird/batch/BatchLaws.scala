package com.twitter.summingbird.batch

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._

import java.util.Date
import java.util.concurrent.TimeUnit

object BatchLaws extends Properties("BatchID") {
  implicit val batchIdArb: Arbitrary[BatchID] =
    Arbitrary { Arbitrary.arbitrary[Long].map { BatchID(_) } }

  property("BatchIDs should RT to String") =
    forAll { batch: BatchID => batch == BatchID(batch.toString) }

  property("BatchID should parse strings as expected") =
    forAll { l: Long => (BatchID("BatchID." + l)) == BatchID(l) }

  property("BatchID should respect ordering") =
    forAll { (a: Long, b: Long) =>
      a.compare(b) == BatchID(a).compare(BatchID(b))
    }

  property("BatchID should respect addition and subtraction") =
    forAll { (init: Long, forward: Long, backward: Long) =>
      val batchID = BatchID(init)
      (batchID + forward - backward) == batchID + (forward - backward)
    }

  property("BatchID should roll forward and backward") =
    forAll { (b: Long) =>
      BatchID(b).next.prev == BatchID(b) &&
      BatchID(b).prev.next == BatchID(b) &&
      BatchID(b).prev == BatchID(b - 1L)
    }

  def batchIdIdentity(batcher : Batcher) =
    { (b : BatchID) => batcher.batchOf(batcher.earliestTimeOf(b)) }

  property("UnitBatcher should always return the same batch") = {
    val batcher = Batcher.unit
    val ident = batchIdIdentity(batcher)
    forAll { batchID: BatchID => ident(batchID) == BatchID(0) }
  }

  val millisPerHour = 1000 * 60 * 60

  def hourlyBatchFloor(batchIdx: Long): Long =
    if (batchIdx >= 0)
      batchIdx * millisPerHour
    else
      batchIdx * millisPerHour + 1

  property("DurationBatcher should Batch correctly") = {
    val batcher = Batcher(1, TimeUnit.HOURS)
    forAll { millis: Long =>
      val hourIndex: Long = millis / millisPerHour

      // Long division rounds toward zero. Add a correction to make
      // sure that our index is floored toward negative inf.
      val flooredBatch = BatchID(if (millis < 0) (hourIndex - 1) else hourIndex)

      (batcher.batchOf(new Date(millis)) == flooredBatch) &&
      (batcher.earliestTimeOf(flooredBatch).getTime ==
        hourlyBatchFloor(flooredBatch.id))
    }
  }
}
