package com.twitter.summingbird.scalding.source

import cascading.flow.FlowDef
import com.twitter.scalding.{ Mode, Mappable, DateRange, RichDate, Hours }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// RangedSource extends ScaldingSource for internal use at
// Twitter. All sources that use this trait will automatically slop
// one hour behind and three hours forward to account for the delay in
// log grouping.

trait RangedSource[Event,Time] extends ScaldingSource[Event,Time] {
  def dateOf(t: Time): RichDate
  def rangedSource(range: DateRange): Mappable[Event]

  override def source(lower: Time, upper: Time)
  (implicit flow: FlowDef, mode: Mode) = {
    // Source one hour behind and three hours forward to compensate
    // for the delay introduced by Twitter's log-grouping.
    val lo = dateOf(lower) - Hours(1)
    val hi = dateOf(upper) + Hours(1)
    rangedSource(DateRange(lo, hi))
  }
}
