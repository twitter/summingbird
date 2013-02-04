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
