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

package com.twitter.summingbird.online

import com.twitter.util.{ Await, Duration, Future, Try }

import java.util.{Queue => JQueue}
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
/**
 *
 * @author Oscar Boykin
 */

object Queue {
  /**
   * By default, don't block on put
   */
  def apply[T]() = linkedNonBlocking[T]

  /** Use this for blocking puts when the size is reached
   */
  def arrayBlocking[T](size: Int): Queue[T] =
    fromBlocking(new ArrayBlockingQueue(size))

  def linkedBlocking[T]: Queue[T] =
    fromBlocking(new LinkedBlockingQueue())

  def linkedNonBlocking[T]: Queue[T] =
    fromQueue(new ConcurrentLinkedQueue())

  def fromBlocking[T](bq: BlockingQueue[T]): Queue[T] = {
    new Queue[T] {
      override def add(t: T) = bq.put(t)
      override def pollNonBlocking = Option(bq.poll())
    }
  }

  // Uses Queue.add to put. This will fail for full blocking queues
  def fromQueue[T](q: JQueue[T]): Queue[T] = {
    new Queue[T] {
      override def add(t: T) = q.add(t)
      override def pollNonBlocking = Option(q.poll())
    }
  }
}

/**
 * Use this class with a thread-safe queue to receive
 * results from futures in one thread.
 * Storm needs us to touch it's code in one event path (via
 * the execute method in bolts)
 */
abstract class Queue[T] {

  /** These are the only two methods to implement.
   * these must be thread-safe.
   */
  protected def add(t: T): Unit
  protected def pollNonBlocking: Option[T]

  private val count = new AtomicInteger(0)

  def dequeueAll(fn: T => Boolean): Seq[T] = {
    val (result, putBack) = toSeq.partition(fn)
    putAll(putBack)
    result
  }

  def put(item: T): Int = {
    add(item)
    count.incrementAndGet
  }

  /** Returns the size immediately after the put */
  def putAll(items: TraversableOnce[T]): Int = {
    val added = items.foldLeft(0) { (cnt, item) =>
      add(item)
      cnt + 1
    }
    count.addAndGet(added)
  }

  /**
   * check if something is ready now
   */
  def poll: Option[T] = {
    val res = pollNonBlocking
    // This is for performance sensitive code. Prefering if to match defensively
    if(res.isDefined) count.decrementAndGet
    res
  }

  /**
   * Obviously, this might not be the same by the time you
   * call trimTo or poll
   */
  def size: Int = count.get

  // Do something on all the elements ready:
  @annotation.tailrec
  final def foreach(fn: T => Unit): Unit =
    poll match {
      case None => ()
      case Some(it) =>
        fn(it)
        foreach(fn)
    }

  // fold on all the elements ready:
  @annotation.tailrec
  final def foldLeft[V](init: V)(fn: (V, T) => V): V =
   poll match {
      case None => init
      case Some(it) => foldLeft(fn(init, it))(fn)
    }


  /** Take all the items currently in the queue */
  def toSeq: Seq[T] = trimTo(0)


  /** Take items currently in the queue up to the max sequence size we want to return
      Due to threading/muliple readers/writers this will not be an exact size
   */

  def take(maxSeqSize: Int): Seq[T] = trimTo(math.max(size - maxSeqSize, 0))

  /**
   * Take enough elements to get .size == maxLength
   */
  def trimTo(maxLength: Int): Seq[T] = {
    require(maxLength >= 0, "maxLength must be >= 0.")

    @annotation.tailrec
    def loop(size: Int, acc: List[T] = Nil): List[T] = {
      if(size > maxLength) {
        pollNonBlocking match {
          case None => acc.reverse // someone else cleared us out
          case Some(item) =>
            loop(count.decrementAndGet, item::acc)
        }
      }
      else acc.reverse
    }
    loop(count.get)
  }
}
