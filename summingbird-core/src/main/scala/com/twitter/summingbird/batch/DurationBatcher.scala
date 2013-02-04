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

import java.util.TimeZone
import com.twitter.bijection.Bijection
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ RichDate, DateOps, AbsoluteDuration }

/**
 * Batcher based on Scalding's AbsoluteDuration.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

abstract class DurationBatcher[Time](duration : AbsoluteDuration) extends BaseDurationBatcher[Time] {
  lazy val tz = TimeZone.getTimeZone("UTC")
  val durationMillis = duration.toMillisecs

  // The DurationBatcher requires strings formatted according ISO8601:
  // http://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations

  def parseTime(s: String) = millisToTime(DateOps.stringToRichDate(s)(tz).timestamp)
}

// Use this class with a Time type of Long when the longs represent milliseconds.
object MillisecondsDurationBatcher {
  def apply(duration: AbsoluteDuration) = new MillisecondsDurationBatcher(duration)
}

class MillisecondsDurationBatcher(duration: AbsoluteDuration) extends DurationBatcher[Long](duration) {
  def timeToMillis(t : Long) = t
  def millisToTime(ms : Long) = ms
  def currentTime = System.currentTimeMillis
}

// Use this class with a Time type of Int when the ints represent seconds.
object SecondsDurationBatcher {
  def apply(duration: AbsoluteDuration) = new SecondsDurationBatcher(duration)
}

class SecondsDurationBatcher(duration: AbsoluteDuration) extends DurationBatcher[Int](duration) {
  def timeToMillis(t : Int) = t * 1000L
  def millisToTime(ms : Long) = (ms / 1000L).toInt
  def currentTime = (System.currentTimeMillis / 1000) toInt
}
