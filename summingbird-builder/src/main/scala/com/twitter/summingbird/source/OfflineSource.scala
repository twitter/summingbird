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

package com.twitter.summingbird.source

import cascading.flow.FlowDef
import com.twitter.scalding.DateRange
import com.twitter.scalding.Mappable

import com.twitter.scalding.Mode
import com.twitter.scalding.TypedPipe
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.tormenta.spout.Spout

import java.util.Date

object OfflineSource {
  def apply[Event](fn: DateRange => Mappable[Event]) =
    new OfflineSource[Event] {
      def scaldingSource(range: DateRange) = fn(range)
    }
}

trait OfflineSource[Event] extends Serializable {
  def scaldingSource(range: DateRange): Mappable[Event]
  def ++(spout: Spout[Event])(implicit mf: Manifest[Event]) = EventSource(this, spout)
}
