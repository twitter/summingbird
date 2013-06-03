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

package com.twitter.summingbird.scalding.source

import cascading.flow.FlowDef
import com.twitter.scalding.{ Mode, Mappable, DateRange, RichDate, Hours }
import java.util.Date

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// RangedSource extends ScaldingSource for internal use at
// Twitter. All sources that use this trait will automatically slop
// one hour behind and three hours forward to account for the delay in
// log grouping.

object RangedSource {
  def apply[Event](fn: DateRange => Mappable[Event]) =
    new RangedSource[Event] {
      def rangedSource(range: DateRange) = fn(range)
    }
}

trait RangedSource[Event] extends ScaldingSource[Event] {
  def rangedSource(range: DateRange): Mappable[Event]

  override def source(lower: Date, upper: Date)(implicit flow: FlowDef, mode: Mode) = {
    // Source one hour behind and three hours forward to compensate
    // for the delay introduced by Twitter's log-grouping.
    val lo = new RichDate(lower) - Hours(1)
    val hi = new RichDate(upper) + Hours(1)
    rangedSource(DateRange(lo, hi))
  }
}
