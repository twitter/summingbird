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

import com.twitter.algebird.{ Monoid, Semigroup, Predecessible, Successible }
import com.twitter.bijection.Bijection
import java.util.Date
import com.twitter.scalding.RichDate

case class Timestamp(milliSinceEpoch: Long) extends AnyVal {
  def compare(that: Timestamp) = milliSinceEpoch.compare(that.milliSinceEpoch)
  def prev = copy(milliSinceEpoch = milliSinceEpoch - 1)
  def next = copy(milliSinceEpoch = milliSinceEpoch + 1)
  def toDate = new Date(milliSinceEpoch)
  def toRichDate = new RichDate(milliSinceEpoch)
  def -(other: Milliseconds) = Timestamp(milliSinceEpoch - other.toLong)
  def +(other: Milliseconds) = Timestamp(milliSinceEpoch + other.toLong)
  // Delta between two timestamps
  def -(other: Timestamp): Milliseconds = Milliseconds(milliSinceEpoch - other.milliSinceEpoch)
  def incrementMillis(millis: Long) = Timestamp(milliSinceEpoch + millis)
  def incrementSeconds(seconds: Long) = Timestamp(milliSinceEpoch + (seconds * 1000L))
  def incrementMinutes(minutes: Long) = Timestamp(milliSinceEpoch + (minutes * 1000 * 60))
  def incrementHours(hours: Long) = Timestamp(milliSinceEpoch + (hours * 1000 * 60 * 60))
  def incrementDays(days: Long) = Timestamp(milliSinceEpoch + (days * 1000 * 60 * 60 * 24))
}

object Timestamp {
  val Max = Timestamp(Long.MaxValue)
  val Min = Timestamp(Long.MinValue)
  def now: Timestamp = Timestamp(System.currentTimeMillis)

  implicit def fromDate(d: Date) = Timestamp(d.getTime)
  implicit val orderingOnTimestamp: Ordering[Timestamp] = Ordering.by(_.milliSinceEpoch)
  implicit val maxTSMonoid: Monoid[Timestamp] = Monoid.from(Timestamp.Min)(orderingOnTimestamp.max(_, _))

  implicit val timestamp2Date: Bijection[Timestamp, Date] =
    Bijection.build[Timestamp, Date] { ts => new Date(ts.milliSinceEpoch) } { fromDate(_) }

  implicit val timestamp2Long: Bijection[Timestamp, Long] =
    Bijection.build[Timestamp, Long] { _.milliSinceEpoch } { Timestamp(_) }

  implicit val timestampSuccessible: Successible[Timestamp] = new Successible[Timestamp] {
    def next(old: Timestamp) = if (old.milliSinceEpoch != Long.MaxValue) Some(old.next) else None
    def ordering: Ordering[Timestamp] = Timestamp.orderingOnTimestamp
    def partialOrdering = Timestamp.orderingOnTimestamp
  }

  implicit val timestampPredecessible: Predecessible[Timestamp] = new Predecessible[Timestamp] {
    def prev(old: Timestamp) = if (old.milliSinceEpoch != Long.MinValue) Some(old.prev) else None
    def ordering: Ordering[Timestamp] = Timestamp.orderingOnTimestamp
    def partialOrdering = Timestamp.orderingOnTimestamp
  }

  // This is a right semigroup, that given any two Timestamps just take the one on the right.
  // The reason we did this is because we don't want to give a stronger contract to the semigroup
  // than the store actually respects
  val rightSemigroup = new Semigroup[Timestamp] {
    def plus(a: Timestamp, b: Timestamp) = b
    override def sumOption(ti: TraversableOnce[Timestamp]) =
      if (ti.isEmpty) None
      else {
        val iter = ti.toIterator
        var last: Timestamp = iter.next
        while (iter.hasNext) {
          last = iter.next
        }
        Some(last)
      }
  }

}
