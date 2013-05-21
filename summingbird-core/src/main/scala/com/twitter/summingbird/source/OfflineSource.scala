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

import com.twitter.scalding.Mode
import com.twitter.scalding.TypedPipe
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.tormenta.spout.ScalaSpout

import java.util.Date

/**
  * OfflineSource[Event] knows how to produce a scalding Pipe
  * representing all the Event objects associated with a given
  * BatchID. The scaldingSource method is responsible for returning a
  * TypedPipe[Event] that contains no events in the BatchID prior to
  * the lowerBound.
  *
  * At Twitter, a typical OfflineSource implementation will map this
  * lowerBound to a timestamp and use the ScaldingEnv to figure out how
  * much data beyond the lowerBound to process for a given run. Since
  * the HDFS log timestamps typically lag a few hours behind the
  * timestamps inside the scribed events, a Twitter
  * OfflineSource[Event,Time] will scoop up a few extra hours of data
  * on each side of the time range calculated off of the BatchIDs and
  * filter out events that don’t belong inside the range. (If the data
  * in HDFS skews more than a couple of hours, this approach can miss
  * data, but this is a weakness of all current Hadoop data pipelines
  * at Twitter and not one that Summingbird aims to solve.)
  *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

trait OfflineSource[Event] extends Serializable {
  def scaldingSource(batcher: Batcher, lowerb: BatchID, env: ScaldingEnv)(timeOf: Event => Date)
    (implicit flow: FlowDef, mode: Mode): TypedPipe[Event]

  def ++(spout: ScalaSpout[Event])(implicit mf: Manifest[Event]) = EventSource(this, spout)
}
