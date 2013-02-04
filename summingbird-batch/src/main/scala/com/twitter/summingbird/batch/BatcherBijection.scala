package com.twitter.summingbird.batch

import java.util.Comparator
import com.twitter.bijection.Bijection

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class BijectedBatcher[T, U](batcher: Batcher[T])(implicit bijection: Bijection[T, U]) extends Batcher[U] {
  import Bijection.asMethod

  def parseTime(s: String) = batcher.parseTime(s).as[U]
  def earliestTimeOf(batch: BatchID) = batcher.earliestTimeOf(batch).as[U]
  def currentTime = batcher.currentTime.as[U]
  def batchOf(t: U) = batcher.batchOf(t.as[T])
  def timeComparator = new Comparator[U] {
    def compare(l: U, r: U) = batcher.timeComparator.compare(l.as[T], r.as[T])
  }
}

class BatcherBijection[T, U](implicit bijection: Bijection[T, U]) extends Bijection[Batcher[T], Batcher[U]] {
  override def apply(batcher: Batcher[T]) = new BijectedBatcher(batcher)
  override def invert(batcher: Batcher[U]) = new BijectedBatcher(batcher)
}
