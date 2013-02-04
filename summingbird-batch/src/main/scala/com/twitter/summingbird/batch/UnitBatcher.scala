package com.twitter.summingbird.batch

/**
* Batcher with a single, infinitely long batch. This makes sense for
* online-only jobs with a single store that will never need to merge
* at the batch level.
*
* We need to pass a value for a lower bound of any Time expected to
* be seen. The time doesn't matter, we just need some instance.
*
* @author Oscar Boykin
* @author Sam Ritchie
*/

object UnitBatcher {
  def apply[Time: Ordering](currentTime: Time): UnitBatcher[Time] = new UnitBatcher(currentTime)
}

class UnitBatcher[Time: Ordering](override val currentTime: Time) extends Batcher[Time] {
  def parseTime(s: String) = currentTime
  def earliestTimeOf(batch: BatchID) = currentTime
  def batchOf(t: Time) = BatchID(0)
  lazy val timeComparator = implicitly[Ordering[Time]]
}
