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
import com.twitter.summingbird.source.OfflineSource
import java.util.Date

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 * This is for compatibility with legacy code. This embiggens the date
 * range requested by 1 hour on each side to catch any spill-over.
 * the source is always filtered before use, so it never uses
 * events with timestamp outside the range.
 */
object RangedSource {
  def apply[Event](fn: DateRange => Mappable[Event]) =
    new OfflineSource[Event] {
      def scaldingSource(range: DateRange) = fn(range.embiggen(Hours(1)))
    }
}
