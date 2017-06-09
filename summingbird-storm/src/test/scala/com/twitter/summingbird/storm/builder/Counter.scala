package com.twitter.summingbird.storm.builder

import java.util.concurrent.atomic.AtomicLong
import com.twitter.algebird.util.summer.Incrementor
/**
 * @author pnaramsetti
 */
case class Counter(name: String) extends Incrementor {
  private val counter = new AtomicLong()

  override def incr: Unit = counter.incrementAndGet()

  override def incrBy(amount: Long): Unit = counter.addAndGet(amount)

  def size = counter.get()

  override def toString: String = s"$name: size:$size"
}
