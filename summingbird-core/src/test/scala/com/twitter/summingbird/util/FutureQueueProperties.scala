/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.util

import com.twitter.util.Future
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._

object FutureQueueProperties extends Properties("FutureQueue") {
  property("FutureQueue should sum values properly") =
    forAll { ints: List[Int] =>
      val initQueue = FutureQueue(Future.value(0), 10)
      val summedFuture =
        ints.foldLeft(initQueue) { (queue, i) =>
          queue.flatMapLast { acc => Future.value(acc + i) }
        }.last
      ints.sum == summedFuture.get
    }
}
