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

package com.twitter.summingbird.batch

import com.twitter.bijection.Bijection
import com.twitter.algebird.Monoid
import com.twitter.util.Duration
import java.util.{ Calendar, Comparator, TimeZone }
import java.text.SimpleDateFormat

/**
 * Base class for duration batchers.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

abstract class BaseDurationBatcher[Time] extends Batcher[Time] {
  def timeToMillis(t: Time): Long
  def millisToTime(millis: Long): Time
  def durationMillis: Long

  def batchOf(t : Time) = {
    val timeInMillis = timeToMillis(t)
    val batch = BatchID(timeInMillis / durationMillis)
    if (timeInMillis < 0L) {
      // Because long division rounds to zero instead of rounding down
      // toward negative infinity, a negative timeInMillis will
      // produce the BatchID AFTER the proper batch. To correct for
      // this, subtract a batch.
      batch.prev
    }
    else {
      batch
    }
  }

  def earliestTimeOf(batch: BatchID) = {
    val id = batch.id
    // Correct for the rounding-to-zero issue described above.
    if(id >= 0L)
      millisToTime(id * durationMillis)
    else
      millisToTime(id * durationMillis + 1L)
  }

  lazy val timeComparator = new Comparator[Time] {
    def compare(l: Time, r: Time) = timeToMillis(l).compareTo(timeToMillis(r))
  }
}
