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

package com.twitter.summingbird.batch

import java.util.{ Calendar, Date, TimeZone }

object CalendarBatcher {
  /** Type wrapper for the Int's defined in java.util.Calendar */
  trait CalField {
    /**
     * This is the java.util.Calendar statir int for this field.
     * Such statics are dangerous to use as they are easy to mistake for counts,
     * thus we use this trait
     */
    def javaIntValue: Int
    /**
     * What is the standard size in milliseconds for this unit of time.
     * Does not need to be precise, but it will improve the speed of
     * unitsSinceEpoch of it is as accurate as possible
     */
    def defaultSize: Long

    def timeZone: TimeZone

    private[this] val cachedCal: ThreadLocal[Calendar] = new ThreadLocal[Calendar] {
      override def initialValue = Calendar.getInstance(timeZone)
    }

    /** the date of exactly cnt units since the epoch */
    def toDate(cnt: Long): Date = {
      val start = cachedCal.get
      start.setTimeInMillis(0L)
      var toCnt = cnt
      while (toCnt != 0L) {
        // the biggest part of this that fits in an Int
        def next(v: Long): Long = {
          if (v < 0) {
            // max is towards zero
            Int.MinValue.toLong max v
          } else {
            Int.MaxValue.toLong min v
          }
        }
        val thisCnt = next(toCnt)
        toCnt -= thisCnt
        start.add(javaIntValue, thisCnt.toInt)
      }
      start.getTime
    }

    /** The floor of the units since the epoch of d */
    def unitsSinceEpoch(d: Date): Long = {

      def notAfter(units: Long): Boolean = !(toDate(units).after(d))

      @annotation.tailrec
      def search(low: Long, upper: Long): Long = {
        if (low == (upper - 1))
          low
        else {
          // mid must be > low because upper >= low + 2
          val mid = low + (upper - low) / 2
          if (notAfter(mid))
            search(mid, upper)
          else
            search(low, mid)
        }
      }

      val guess = d.getTime / defaultSize
      // Don't linearly search for near times, skip this many defaultSized milliseconds
      val skip = 10L
      @annotation.tailrec
      def makeBefore(prev: Long = guess): Long = {
        if (notAfter(prev)) prev
        else makeBefore(prev - skip)
      }
      @annotation.tailrec
      def makeAfter(prev: Long = guess): Long = {
        if (!notAfter(prev)) prev
        else makeAfter(prev + skip)
      }
      // Return the largest value that is not after the date (<=)
      search(makeBefore(), makeAfter())
    }
  }

  def hoursField(tz: TimeZone): CalField = new CalField {
    def javaIntValue = Calendar.HOUR
    def defaultSize = 1000L * 60L * 60L
    def timeZone = tz
  }

  def daysField(tz: TimeZone): CalField = new CalField {
    def javaIntValue = Calendar.DAY_OF_YEAR
    def defaultSize = 1000L * 60L * 60L * 24L
    def timeZone = tz
  }

  def ofDays(days: Int)(implicit tz: TimeZone): CalendarBatcher =
    CalendarBatcher(days, daysField(tz))

  def ofHours(hours: Int)(implicit tz: TimeZone): CalendarBatcher =
    CalendarBatcher(hours, hoursField(tz))

  def ofDaysUtc(days: Int): CalendarBatcher =
    ofDays(days)(TimeZone.getTimeZone("UTC"))

  def ofHoursUtc(hours: Int): CalendarBatcher =
    ofHours(hours)(TimeZone.getTimeZone("UTC"))
}

/**
 * This batcher numbers batches based on a Calendar, not just milliseconds.
 * Many offline HDFS sources at Twitter are batched in this way based on hour
 * or day in the UTC calendar
 */
final case class CalendarBatcher(unitCount: Int, calField: CalendarBatcher.CalField) extends Batcher {
  final def batchOf(t: Timestamp) = BatchID(calField.unitsSinceEpoch(t.toDate) / unitCount)
  final def earliestTimeOf(batch: BatchID) = calField.toDate(batch.id.toInt * unitCount)
}
