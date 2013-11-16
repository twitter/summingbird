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

import com.twitter.util.{ Await, Future }
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * A thread-safe queue that spills after the size is bigger
 * than a max-size.
 */
class BoundedQueue[T](maxLength: Int) {
  require(maxLength >= 0, "maxLength must be >= 0.")

  private val queue = new ConcurrentLinkedQueue[T]()
  private val count = new AtomicInteger(0)

  /** Returns the size immediately after the put */
  def putAll(items: TraversableOnce[T]): Int = {
    val added = items.foldLeft(0) { (cnt, item) =>
      queue.add(item)
      cnt + 1
    }
    count.addAndGet(added)
  }

  /** Returns the size immediately after the put */
  def put(item: T): Int = {
    queue.add(item)
    count.incrementAndGet
  }

  /**
   * Obviously, this might not be the same by the time you
   * call spill
   */
  def size: Int = count.get

  /**
   * Take enough elements to get the queue under the maxLength
   */
  def spill: Seq[T] = spill(count.get)

  @annotation.tailrec
  private def spill(size: Int, acc: List[T] = Nil): List[T] = {
    if(size > maxLength) {
      queue.poll match {
        case null => acc.reverse // someone else cleared us out
        case item =>
          spill(count.decrementAndGet, item::acc)
      }
    }
    else acc.reverse
  }
}
