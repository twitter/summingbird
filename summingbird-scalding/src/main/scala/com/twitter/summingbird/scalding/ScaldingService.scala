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

package com.twitter.summingbird.scalding

import com.twitter.summingbird.batch.{ BatchID, Batcher, Interval }
import com.twitter.scalding.{Mode, TypedPipe}
import cascading.flow.FlowDef

trait ScaldingService[K, V] {
  implicit def ordering: Ordering[K]
  // The batcher that describes this service
  def batcher: Batcher
  // A static, or write-once service can  potentially optimize this without writing the (K, V) stream out
  def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))]
}

trait MaterializedService[K, V] extends ScaldingService[K, V] {
  /** Reads the key log for this batch. By key log we mean a log of time, key and value when the key was written
   * to. This is an associative operation and sufficient to scedule the service.
   *
   * May include keys from previous batches, since we need to be able to look back in time to the most recent
   * value for each key that exists.
   */
  def readStream(batchID: BatchID)(implicit flowdef: FlowDef, mode: Mode): KeyValuePipe[K, V]
  def lookup[W](lowerIn: BatchID, upperEx: BatchID, getKeys: KeyValuePipe[K, W])(implicit flowdef: FlowDef, mode: Mode): Try[KeyValuePipe[K, (W, Option[V])]] =
    sys.error("TODO: this can be implemented as the lookup join, if you have materialized the K,V stream")
}
