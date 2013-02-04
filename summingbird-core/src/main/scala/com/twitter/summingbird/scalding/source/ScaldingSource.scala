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
import com.twitter.scalding.{ Mode, Dsl, TDsl }
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.source.OfflineSource

import com.twitter.scalding.Mappable

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * A ScaldingSource is an OfflineSource implemented in terms of bounds on Time,
 * rather than the actual methods on the batcher.
 */

trait ScaldingSource[Event,Time] extends OfflineSource[Event,Time] {
  import Dsl._
  import TDsl._

  // The source method receives a lower and upper bound on time and
  // must return a scalding Mappable guaranteed to produce ALL Event
  // instances that fall within the bounds. Please err on the side of
  // producing Events outside the range; Summingbird will filter these
  // out.
  //
  // (At Twitter, Events are stored in HDFS folders partitioned by
  // hour or day. The hour or day of the folder is often shifted by a
  // few hours from the Time inside of the actual Event due to the
  // delay introduced by the log-partitioning Hadoop job. Twitter
  // "source" implementations should source a few extra folders on
  // each side of the bound. We don't currently have a better way of
  // sourcing Events by time range. The more conservative should pull
  // in more slop hours.)
  def source(lower: Time, upper: Time)
  (implicit flow: FlowDef, mode: Mode): Mappable[Event]

  // scaldingSource implemented in terms of the source method
  // above. Note that events outside the range defined by "lower" and
  // "upper" are filtered out here, so slop is okay and encouraged.
  override def scaldingSource(batcher: Batcher[Time], lowerBound: BatchID, env: ScaldingEnv)
  (implicit flow: FlowDef, mode: Mode) = {
    val lowerTime = batcher.earliestTimeOf(lowerBound)
    val upperBound = lowerBound + env.batches
    val upperTime = batcher.earliestTimeOf(upperBound)

    mappableToTypedPipe(source(lowerTime, upperTime))
      .filter { e =>
        val batchID = batcher.batchOf(timeOf(e))
        lowerBound <= batchID && batchID < upperBound
      }
  }
}
