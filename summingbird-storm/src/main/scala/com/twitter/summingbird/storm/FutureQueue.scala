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

package com.twitter.summingbird.storm

import com.twitter.util.{ Await, Future }
import scala.collection.mutable.{ Queue => MutableQueue }

/**
 * Maintains a rolling window of futures. Future # n is
 * forced after Future (n + maxLength) is added to the
 * queue.
 *
 * TODO (https://github.com/twitter/summingbird/issues/83): remove
 * this in favor of BufferingStore in storehaus after further
 * performance investigation.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

case class FutureQueue[T](init: Future[T], maxLength: Int) {
  require(maxLength >= 1, "maxLength cannot be negative.")
  private val queue = MutableQueue[Future[T]](init)

  def +=(future: Future[T]): this.type = {
    queue += future
    // Force extra futures.
    while (queue.length > maxLength) { Await.result(queue.dequeue) }

    // Drop all realized futures but the head off the tail
    while(queue.size > 1 && queue.head.isDefined) { queue.dequeue }
    this
  }
}
