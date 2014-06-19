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

import com.twitter.bijection.Injection
import com.twitter.summingbird.builder.SourceBuilder
import com.twitter.tormenta.spout.Spout

import java.util.Date

/**
 * An EventSource[Event,Time] is a compound source that mixes together
 * the notions of an online and offline source:
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

case class EventSource[T: Manifest](offline: Option[OfflineSource[T]], spout: Option[Spout[T]]) {
  def withTime(fn: T => Date)(implicit inj: Injection[T, Array[Byte]]): SourceBuilder[T] =
    SourceBuilder(this, fn)
}

object EventSource {
  def fromOffline[T: Manifest](offline: OfflineSource[T]): EventSource[T] = new EventSource(Some(offline), None)
  def fromOnline[T: Manifest](spout: Spout[T]): EventSource[T] = new EventSource(None, Some(spout))
  def apply[T: Manifest](offline: OfflineSource[T], spout: Spout[T]): EventSource[T] = new EventSource(Some(offline), Some(spout))
}
