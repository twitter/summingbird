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
import scala.collection.mutable.{Queue => MQueue}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
/**
 *
 * @author Oscar Boykin
 */


case class Queue[T]() {
  val backingQueue = MQueue[T]()

  protected def add(t: T) {
    this.synchronized {
      backingQueue += t
    }
  }

  def put(item: T): Int = {
    add(item)
    backingQueue.size
  }

  /** Returns the size immediately after the put */
  def putAll(items: TraversableOnce[T]): Int = {
    items.foreach(add(_))
    size
  }

  /**
   * Obviously, this might not be the same by the time you
   * call trimTo or poll
   */
  def size: Int = backingQueue.size

  final def dequeueWhere(fn: T => Boolean): Seq[T] =
    this.synchronized {
      backingQueue.dequeueAll(fn)
    }


  final def foreach(fn: T => Unit): Unit =
    backingQueue.dequeueAll(x => true).foreach(fn(_))

  def trimTo(maxLength: Int): Seq[T] = {
    require(maxLength >= 0, "maxLength must be >= 0.")


    @annotation.tailrec
    def loop(acc: List[T] = Nil): List[T] = {
      if(backingQueue.size > maxLength) {
        loop(backingQueue.dequeue :: acc)
      }
      else acc.reverse
    }
    this.synchronized {
      loop()
    }
  }
}