package com.twitter.summingbird.akka

import com.twitter.util.Future

object AkkaSource {
  def fromTraversable[T](items: TraversableOnce[T]): AkkaSource[T] =
    new AkkaSource[T] {
      private var finished = false
      override def isFinished = finished
      override def poll = {
        finished = true
        Future.value(items)
      }
    }

  def fromFn[T](fn: () => TraversableOnce[T]): AkkaSource[T] =
    new AkkaSource[T] {
      private var finished = false
      override def isFinished = finished

      override def poll = {
        finished = true
        Future.value(fn.apply)
      }
    }
}

trait AkkaSource[+T] extends Serializable { self =>
  def open() {}

  def isFinished: Boolean
  /**
   * Override to supply new tuples.
   */
  def poll: Future[TraversableOnce[T]]

  def filter(fn: T => Boolean): AkkaSource[T] =
    flatMap[T](t => if (fn(t)) Some(t) else None)

  def map[U](fn: T => U): AkkaSource[U] =
    flatMap(t => Some(fn(t)))

  def flatMap[U](fn: T => TraversableOnce[U]) =
    new AkkaSource[U] {
      override def isFinished = self.isFinished
      override def poll = self.poll.map{y => y.flatMap(fn(_))}
    }
}
