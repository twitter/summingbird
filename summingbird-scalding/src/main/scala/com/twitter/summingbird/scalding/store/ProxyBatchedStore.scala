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

package com.twitter.summingbird.scalding.store

import cascading.flow.FlowDef
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding._
import com.twitter.scalding.{ Mode, TypedPipe }

/**
 * Use this class to easily change, for instance, the pruning
 * for an existing store.
 */
abstract class ProxyBatchedStore[K, V] extends batch.BatchedStore[K, V] {
  def proxy: batch.BatchedStore[K, V]
  override def batcher = proxy.batcher
  override def ordering = proxy.ordering
  override def select(b: List[BatchID]) = proxy.select(b)
  override def pruning = proxy.pruning
  override def readLast(exclusiveUB: BatchID, mode: Mode) = proxy.readLast(exclusiveUB, mode)
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit =
    proxy.writeLast(batchID, lastVals)(flowDef, mode)

  override def toString = "ProxyBatchedStore(proxyingFor=%s)".format(proxy.toString)
}
