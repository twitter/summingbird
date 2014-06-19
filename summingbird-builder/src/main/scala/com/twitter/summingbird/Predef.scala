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
 * Aliases for types and objects commonly used in Summingbird
 * jobs. Importing these makes it easier to define jobs:
 *
 * {{{
 * import com.twitter.summingbird.Predef._
 *
 * class MyJob(env: Env) extends AbstractJob {
 *   import MyJob._  // assumed to hold flatmapper and sources
 *
 * implicit val batcher: Batcher = Batcher.ofHours(2)
 * // Now, job creation is easy!
 *
 * }
 * }}}
 *
 * @author Sam Ritchie
 */
object Predef {
  // Core types
  type AbstractJob = com.twitter.summingbird.AbstractJob
  type Env = com.twitter.summingbird.Env

  // Batcher-related types and objects
  type BatchID = com.twitter.summingbird.batch.BatchID
  type Batcher = com.twitter.summingbird.batch.Batcher
  val Batcher = com.twitter.summingbird.batch.Batcher

  // Core types
  type CompoundStore[K, V] = com.twitter.summingbird.store.CompoundStore[K, V]
  val CompoundStore = com.twitter.summingbird.store.CompoundStore

  // Offline stores
  type VersionedStore[K, V] = com.twitter.summingbird.scalding.store.VersionedBatchStore[K, V, K, (BatchID, V)]
  val VersionedStore = com.twitter.summingbird.scalding.store.VersionedStore

  // Common options
  type CacheSize = com.twitter.summingbird.option.CacheSize
  val CacheSize = com.twitter.summingbird.option.CacheSize
}
