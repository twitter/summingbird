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

import java.util.Queue
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
/**
 *
 * @author Oscar Boykin
 */

object Channel {
  /**
   * By default, don't block on put
   */
  def apply[T]() = linkedNonBlocking[T]

  def arrayBlocking[T](size: Int): Channel[T] =
    fromBlocking(new ArrayBlockingQueue(size))

  def linkedBlocking[T]: Channel[T] =
    fromBlocking(new LinkedBlockingQueue())

  def linkedNonBlocking[T]: Channel[T] =
    fromQueue(new ConcurrentLinkedQueue())

  def fromBlocking[T](bq: BlockingQueue[T]): Channel[T] = {
    new Channel[T] {
      override def add(t: T) = bq.put(t)
      override def pollOrNull = bq.poll()
    }
  }

  // Uses Queue.add to put. This will fail for full blocking queues
  def fromQueue[T](q: Queue[T]): Channel[T] = {
    new Channel[T] {
      override def add(t: T) = q.add(t)
      override def pollOrNull = q.poll()
    }
  }
}

/**
 * Use this class with a thread-safe queue to receive
 * results from futures in one thread.
 * Storm needs us to touch it's code in one event path (via
 * the execute method in bolts)
 */
abstract class Channel[T] {

  protected def add(t: T): Unit
  protected def pollOrNull: T

  private val count = new AtomicInteger(0)

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
  def poll: Option[T] = Option(pollOrNull)

  /**
   * Obviously, this might not be the same by the time you
   * call spill
   */
  def size: Int = count.get

  // Do something on all the elements ready:
  @annotation.tailrec
  final def foreach(fn: T => Unit): Unit =
    pollOrNull match {
      case null => ()
      case itt => fn(itt); foreach(fn)
    }

  // fold on all the elements ready:
  @annotation.tailrec
  final def foldLeft[V](init: V)(fn: (V, T) => V): V = {
   pollOrNull match {
      case null => init
      case itt => foldLeft(fn(init, itt))(fn)
    }
  }

  /**
   * Take enough elements to get the queue under the maxLength
   */
  def trimTo(maxLength: Int): Seq[T] = {
    require(maxLength >= 0, "maxLength must be >= 0.")

    @annotation.tailrec
    def loop(size: Int, acc: List[T] = Nil): List[T] = {
      if(size > maxLength) {
        pollOrNull match {
          case null => acc.reverse // someone else cleared us out
          case item =>
            loop(count.decrementAndGet, item::acc)
        }
      }
      else acc.reverse
    }
    loop(count.get)
  }
}
