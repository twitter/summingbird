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
trait UniqueKeyedService[K, V] extends ScaldingService[K, V] {

  /** Given a requested DateRange, from which all inputs to the service will come,
   * and Mode, tell us what is the largest subset of
   * this range that can be satisfied, or give an Error
   */
  def readDateRange(requested: DateRange, mode: Mode): Try[(DateRange, FlowProducer[TypedPipe[(K, V)]])]
  def ordering: Ordering[K]

  def doJoin[W](in: TypedPipe[(Time, (K, W))],
    serv: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[(Time, (K, (W, Option[V])))] = {
      implicit val ord: Ordering[K] = ordering
      in.map { case (t, (k, w)) => (k, (t, w)) }
        .group
        .leftJoin(serv.group)
        .toTypedPipe
        .map { case (k, ((t, w), optV)) => (t, (k, (w, optV))) }
    }

  final def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))] = {
    StateWithError({ intMode: FactoryInput =>
      val (timeSpan, mode) = intMode
      import Scalding.dateRangeInjection
      timeSpan.as[Option[DateRange]]
        .map { Right(_) }
        .getOrElse(Left(List("could not convert timespan to range: " + timeSpan)))
        .right.flatMap { dr => readDateRange(dr, mode) }
        .right.map { case (dr, fp) => (dr.as[Interval[Time]], fp) }
        .right.flatMap { case (intr, fp) =>
          getKeys((intr, mode))
            .right
            .map { case ((avail, m), getFlow) =>
              val rdr = Reader({ implicit fdM: (FlowDef, Mode) =>
                val input: TypedPipe[(Long, (K, W))] = getFlow(fdM)
                val serv: TypedPipe[(K, V)] = fp(fdM)
                doJoin(input, serv)
              })
              ((avail, m), rdr)
            }
        }
    })
  }
}

trait SourceUniqueKeyedService[S <: SSource, K, V] extends UniqueKeyedService[K, V] {
  def source(dr: DateRange): S
  def toPipe(s: S)(implicit flow: FlowDef, mode: Mode): TypedPipe[(K,V)]

  final def readDateRange(req: DateRange, mode: Mode) =
    Scalding.minify(mode, req)(source(_)).right.map { dr =>
      (dr, Reader({implicit fdM: (FlowDef, Mode) =>
        toPipe(source(dr))
      }))
    }
}

object UniqueKeyedService extends java.io.Serializable {
  def from[K:Ordering,V](fn: DateRange => Mappable[(K,V)]): UniqueKeyedService[K, V] =
    fromAndThen[(K,V),K,V](fn, identity)

  /** The Mappable is the subclass of Source that knows about the file system. */
  def fromAndThen[T,K:Ordering,V](fn: DateRange => Mappable[T], andThen: TypedPipe[T] => TypedPipe[(K,V)]): UniqueKeyedService[K, V] =
    new SourceUniqueKeyedService[Mappable[T], K, V] {
      def ordering = Ordering[K]
      def source(dr: DateRange) = fn(dr)
      def toPipe(mappable: Mappable[T])(implicit flow: FlowDef, mode: Mode) =
        andThen(TypedPipe.from(mappable)(flow, mode, mappable.converter)) // converter is removed in 0.9.0
    }
}
