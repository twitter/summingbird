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

package com.twitter.summingbird.example

import com.twitter.summingbird._
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.MergeableStoreFactory
import com.twitter.summingbird.storm.Storm
import twitter4j.Status
import twitter4j.TwitterStreamFactory
import twitter4j.conf.ConfigurationBuilder

object StatusStreamer {
  /**
   * These two items are required to run Summingbird in
   * batch/realtime mode, across the boundary between storm and
   * scalding jobs.
   */
  implicit val timeOf: TimeExtractor[Status] = TimeExtractor(_.getCreatedAt.getTime)
  implicit val batcher = Batcher.ofHours(1)

  def tokenize(text: String): TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")

  /**
   * The actual Summingbird job. Notice that the execution platform
   * "P" stays abstract. This job will work just as well in memory,
   * in Storm or in Scalding, or in any future platform supported by
   * Summingbird.
   */
  def wordCount[P <: Platform[P]](
    source: Producer[P, Status],
    store: P#Store[String, Long]) =
    source
      .filter(_.getText != null)
      .flatMap { tweet: Status => tokenize(tweet.getText).map(_ -> 1L) }
      .sumByKey(store)
}
