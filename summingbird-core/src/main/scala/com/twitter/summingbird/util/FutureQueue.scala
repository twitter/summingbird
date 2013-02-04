package com.twitter.summingbird.util

import com.twitter.util.Future
import scala.collection.mutable.{ Queue => MutableQueue }

/**
 * Maintains a rolling window of futures. Future # n is
 * forced after Future (n + maxLength) is added to the
 * queue (via flatMapLast).
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

case class FutureQueue[T](init: Future[T], maxLength: Int) {
  require(maxLength >= 1, "maxLength cannot be negative.")
  private val queue = MutableQueue[Future[T]](init)

  private def +=(future: Future[T]): this.type = {
    queue += future
    // Force extra futures.
    while (queue.length > maxLength) { queue.dequeue.apply }

    // Drop all realized futures but the head off the tail
    while(queue.size >= 1 && queue.head.isDefined) { queue.dequeue }
    this
  }

  def last: Future[T] = queue.last
  def transformLast(fn: Future[T] => Future[T]): this.type =
    this += fn(queue.last)
  def flatMapLast(fn: T => Future[T]): this.type =
    this += queue.last.flatMap(fn)
}
