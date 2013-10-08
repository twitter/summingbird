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
import com.twitter.algebird.monad.{Reader, StateWithError}
import com.twitter.algebird.Interval

import com.twitter.bijection.Conversion.asMethod

import com.twitter.scalding.{Source => SSource, _}
import cascading.flow.FlowDef

/** A UniqueKeyedService covers the case where Keys are globally
 * unique and either are not present or have one value.
 * Examples could be Keys which are Unique IDs, such as UserIDs,
 * content IDs, cryptographic hashes, etc...
 */

trait SimpleService[K, V] extends ScaldingService[K, V] {

  import Scalding.dateRangeInjection

  /** Return the maximum subset of the requested range that can be handled */
  def satisfiable(requested: DateRange, mode: Mode): Try[DateRange]

  def serve[K1<:K,W](covering: DateRange,
    input: TypedPipe[(Long, (K1, W))])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[(Long, (K1, (W, Option[V])))]

  final def lookup[K1<:K,W](getKeys: PipeFactory[(K1, W)]): PipeFactory[(K1, (W, Option[V]))] =
    StateWithError({ intMode: FactoryInput =>
      val (timeSpan, mode) = intMode
      Scalding.toDateRange(timeSpan).right
        .flatMap(satisfiable(_, mode)).right
        .flatMap { dr =>
          val ts = dr.as[Interval[Time]]
          getKeys((ts, mode)).right
            .map { case ((avail, m), getFlow) =>
              val rdr = Reader({ implicit fdM: (FlowDef, Mode) =>
                // This get can't fail because it came from a DateRange initially
                serve(avail.as[Option[DateRange]].get, getFlow(fdM))
              })
              ((avail, m), rdr)
            }
        }
    })
}
