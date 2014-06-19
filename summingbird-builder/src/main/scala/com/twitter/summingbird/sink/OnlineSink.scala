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

package com.twitter.summingbird.sink

import com.twitter.util.Future

/**
 * Represents a location to which intermediate results of the
 * "flatMap" operation can be written for consumption by other
 * jobs. This sink can be implemented using, for example, a kestrel
 * fanout or kafka topic.
 */
trait OnlineSink[-Event] extends (Event => Future[Unit]) {
  /**
   * Note that the flatMap operation WILL error if this future errors, so be sure
   * to handle appropriate exceptions here.
   */
  def write(event: Event): Future[Unit]
  final def apply(e: Event) = write(e)
}

object OnlineSink {
  val unit: OnlineSink[Any] = EmptyOnlineSink
}

object EmptyOnlineSink extends OnlineSink[Any] {
  def write(event: Any) = Future.Unit
}
