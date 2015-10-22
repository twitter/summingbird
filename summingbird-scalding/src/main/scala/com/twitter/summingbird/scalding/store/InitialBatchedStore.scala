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

import com.twitter.scalding.{ Mode, TypedPipe }
import com.twitter.summingbird._
import com.twitter.summingbird.scalding.batch.BatchedStore
import com.twitter.summingbird.scalding.{ Try, FlowProducer, Scalding }
import com.twitter.summingbird.batch.{ BatchID, OrderedFromOrderingExt }
import cascading.flow.FlowDef
import com.twitter.summingbird.scalding._

/**
 * For (firstNonZero - 1) we read empty. For all before we error on read. For all later, we proxy
 * On write, we throw if batchID is less than firstNonZero
 */
class InitialBatchedStore[K, V](val firstNonZero: BatchID, override val proxy: BatchedStore[K, V])
    extends ProxyBatchedStore[K, V] {
  import OrderedFromOrderingExt._
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode) =
    if (batchID >= firstNonZero) proxy.writeLast(batchID, lastVals)
    else sys.error("Earliest batch set at :" + firstNonZero + " but tried to write: " + batchID)

  // Here is where we switch:
  override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    if (exclusiveUB > firstNonZero) proxy.readLast(exclusiveUB, mode)
    else if (exclusiveUB == firstNonZero) Right((firstNonZero.prev, Scalding.emptyFlowProducer[(K, V)]))
    else Left(List("Earliest batch set at :" + firstNonZero + " but tried to read: " + exclusiveUB))
  }

  override def toString =
    "InitialBatchedStore(firstNonZero=%s, proxyingFor=%s)".format(firstNonZero.toString, proxy.toString)
}
