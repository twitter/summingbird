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

import com.twitter.bijection.Bijection.asMethod // "as" syntax
import com.twitter.util.Duration
import java.util.{ Calendar, Comparator, TimeZone }
import java.text.SimpleDateFormat
import java.lang.{ Integer => JInt, Long => JLong }

/**
 * Batcher based on com.twitter.util.Duration.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

abstract class UtilDurationBatcher[Time](duration: Duration) extends BaseDurationBatcher[Time] {
  val durationMillis = duration.inMilliseconds

  val TIMEZONE = "UTC"
  val FORMAT_STRING = "yyyy-MM-dd HH:mm:ss"

  // TODO: Extract the date parsing logic from Scalding into a common
  // location to allow for richer date formats.
  def parseTime(s: String) = {
    val cal = Calendar.getInstance(TimeZone.getTimeZone(TIMEZONE))
    val formatter = new SimpleDateFormat(FORMAT_STRING)

    formatter.setCalendar(cal)
    val newStr = s
      .replace("T"," ") //We allow T to separate dates and times, just remove it and then validate
      .replaceAll("[/_]", "-")  // Allow for slashes and underscores
    //We allow T to separate dates and times, just remove it and then validate
    millisToTime(formatter.parse(newStr).getTime)
  }

  def parseTime(formatString: String, s: String) = {
    val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    val formatter = new SimpleDateFormat(formatString)
    formatter.setCalendar(cal)
    formatter.parse(s)
  }
}

// Use this class with a Time type of Long when the longs represent milliseconds.
object MillisecondsUtilDurationBatcher {
  def apply(duration: Duration): Batcher[Long] = new MillisecondsUtilDurationBatcher(duration)
  def ofJavaLong(duration: Duration): Batcher[JLong] = apply(duration).as[Batcher[JLong]]
}

class MillisecondsUtilDurationBatcher(duration: Duration) extends UtilDurationBatcher[Long](duration) {
  def timeToMillis(t: Long) = t
  def millisToTime(ms: Long) = ms
  def currentTime = System.currentTimeMillis
}

// Use this class with a Time type of Int when the ints represent seconds.
object SecondsUtilDurationBatcher {
  def apply(duration: Duration): Batcher[Int] = new SecondsUtilDurationBatcher(duration)
  def ofJavaInt(duration: Duration): Batcher[JInt] = apply(duration).as[Batcher[JInt]]
}

class SecondsUtilDurationBatcher(duration: Duration) extends UtilDurationBatcher[Int](duration) {
  def timeToMillis(t: Int) = t * 1000L
  def millisToTime(ms: Long) = (ms / 1000L).toInt
  def currentTime = (System.currentTimeMillis / 1000) toInt
}
