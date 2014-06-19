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

package com.twitter.summingbird

/**
 * The "example" package contains all of the code and configuration
 * necessary to run a basic Summingbird job locally that consumes
 * Tweets from the public streaming API and generates counts of the
 * number of times each word appears per tweet.
 *
 * Clients can use the code exposed in this example to build realtime
 * versions of word-count dashboards like Google's N-Gram product:
 *
 * http://books.google.com/ngrams
 *
 * # Code Structure
 *
 * ## Serialization.scala
 *
 * defines a number of serialization Injections
 * needed by the Storm and Scalding platforms to ensure that data can
 * move across network boundaries without corruption.
 *
 * ## Storage.scala
 *
 * Defines a few helper methods that make it easy to instantiate
 * instances of MergeableStore backed by Memcache.
 *
 * ## ExampleJob.scala
 *
 * The actual Summingbird job, plus a couple of helper implicits (a
 * batcher and a time extractor) necessary for running jobs in
 * combined batch/realtime mode across Scalding and Storm.
 *
 * ## StormRunner.scala
 *
 * Configuration and Execution of the summingbird word count job in
 * Storm's local mode, plus some advice on how to test that Storm is
 * populating the Memcache store with good counts.
 *
 *
 * # Have Fun!
 *
 * Please send any questions on the code you see here to
 * sritchie@twitter.com, or send me a tweet at @sritchie. Once you
 * get this code compiling and running, you'll be cooking with gas!
 */
package object example {}
