/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.summingbird.batch

import com.twitter.algebird.{Interval, InclusiveUpper, Empty}

trait TimeBoundedKeySpace[-K] extends java.io.Serializable {
  // if this is true, this key is never updated during the passed interval
  // use case: for time series dashboards. The value is kept, but we
  // see that we don't need to look for more updates to the key
  def isFrozen(key: K, period: Interval[Timestamp]): Boolean
}

object TimeBoundedKeySpace extends java.io.Serializable {
  val neverFrozen: TimeBoundedKeySpace[Any] =
    new TimeBoundedKeySpace[Any] { def isFrozen(key: Any, period: Interval[Timestamp]) = false }

  def apply[K](fn: (K, Interval[Timestamp]) => Boolean) = new TimeBoundedKeySpace[K] {
    def isFrozen(key: K, period: Interval[Timestamp]) = fn(key, period)
  }

  /**
   * This is a common case of being able to compute a time after which
   * no more writes occur.
   */
  def freezesAt[K](ftime: K => Timestamp): TimeBoundedKeySpace[K] =
    new TimeBoundedKeySpace[K] {
      def isFrozen(key: K, period: Interval[Timestamp]) = {
        val liquid = InclusiveUpper(ftime(key))
        (liquid && period) == Empty[Timestamp]()
      }
    }
}
