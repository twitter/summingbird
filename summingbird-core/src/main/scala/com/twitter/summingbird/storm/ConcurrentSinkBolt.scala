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

package com.twitter.summingbird.storm

import com.twitter.algebird.Monoid
import com.twitter.bijection.Bijection
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.builder._
import com.twitter.storehaus.Store
import com.twitter.summingbird.util.CacheSize
import com.twitter.util.Future

/**
 * Extension of SinkBolt that allows concurrent writes from multiIncrement.
 * WARNING: Only use this with ConcurrentMutableStores. The type bounds with
 * type erasure (in the CompleteBuilder pattern match) don't allow the type
 * system to enforce this constraint.
 *
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// TODO: Put "multiSet" on Store inside of Storehaus and remove this thing.
class ConcurrentSinkBolt[StoreType <: Store[StoreType, (Key,BatchID), Value], Key, Value: Monoid]
(@transient store: StoreType,
 @transient rpc: Bijection[Option[Value], String],
 @transient successHandler: OnlineSuccessHandler,
 @transient exceptionHandler: OnlineExceptionHandler,
 cacheSize: CacheSize,
 maxWaitingFutures: MaxWaitingFutures,
 metrics: SinkStormMetrics)
extends SinkBolt[StoreType, Key, Value](store, rpc, successHandler, exceptionHandler,
                                        cacheSize, maxWaitingFutures, metrics) {

  override protected def multiIncrement(store: StoreType, items: Map[(Key,BatchID),Value]) =
    Future.collect(items.toSeq map { item => increment(store, item) }) map { _ => store }
}
