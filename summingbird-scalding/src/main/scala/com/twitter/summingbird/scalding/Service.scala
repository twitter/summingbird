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

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.{ Dependants, Producer, Summer }
import com.twitter.summingbird.scalding.batch.BatchedStore

sealed trait Service[K, +V] extends java.io.Serializable

/**
 * This represents a service that is *external* to the current job.
 * This does not include joins for data that is generated in the same
 * Producer graph
 */
trait ExternalService[K, +V] extends Service[K, V] {
  // A static, or write-once service can  potentially optimize this without writing the (K, V) stream out
  def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))]
}

/**
 * This represents a join against data that is materialized by a store
 * in the current job
 */
sealed trait InternalService[K, +V] extends Service[K, V]
case class StoreService[K, V](store: BatchedStore[K, V]) extends InternalService[K, V]

/**
 * Here are some methods that are useful in planning the execution of Internal Services
 */
private[scalding] object InternalService {
  /**
   * This returns true if the dependants of the left does not
   * contain the store
   */
  def doesNotDependOnStore[K, V](left: Producer[Scalding, Any],
    store: BatchedStore[K, V]): Boolean =
    Producer.transitiveDependenciesOf(left)
      .collectFirst { case Summer(_, thatStore, _) if thatStore == store => () }
      .isDefined

  def storeIsJoined[K, V](dag: Dependants[Scalding], store: Store[K, V]): Boolean =
    sys.error("?")

  def getSummer[K, V](dag: Dependants[Scalding],
    store: BatchedStore[K, V]): Option[Summer[Scalding, K, V]] =
    sys.error("?")

  /**
   * Just wire in LookupJoin here. This method assumes that
   * the FlowToPipe is already on the matching time, so we don't
   * need to worry about that here.
   */
  def doIndependentJoin[K, U, V](input: FlowToPipe[(K, U)],
    toJoin: FlowToPipe[(K, V)],
    sg: Semigroup[V]): FlowToPipe[(K, (U, Option[V]))] =
    sys.error("?")
}
