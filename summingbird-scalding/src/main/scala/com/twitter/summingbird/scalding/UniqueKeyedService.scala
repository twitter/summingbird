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

import com.twitter.scalding.{Source => SSource, _ }
import com.twitter.scalding.typed.{ TypedPipe => _, _ }
import cascading.flow.FlowDef

/** A UniqueKeyedService covers the case where Keys are globally
 * unique and either are not present or have one value.
 * Examples could be Keys which are Unique IDs, such as UserIDs,
 * content IDs, cryptographic hashes, etc...
 */
trait UniqueKeyedService[K, V] extends SimpleService[K, V] {

  /** Load the range of data to do a join */
  def readDateRange(requested: DateRange)(implicit flowDef: FlowDef, mode: Mode): TypedPipe[(K, V)]
  def ordering: Ordering[K]
  def reducers: Option[Int]

  /** You can override this to use hashJoin for instance */
  def doJoin[W](in: TypedPipe[(Time, (K, W))],
    serv: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[(Time, (K, (W, Option[V])))] = {
      implicit val ord: Ordering[K] = ordering
      def withReducers[U, T](grouped: Grouped[U, T]) =
        reducers.map { grouped.withReducers(_) }.getOrElse(grouped)

      withReducers(in.map { case (t, (k, w)) => (k, (t, w)) }.group)
        .leftJoin(withReducers(serv.group))
        .toTypedPipe
        .map { case (k, ((t, w), optV)) => (t, (k, (w, optV))) }
    }

  final override def serve[W](covering: DateRange,
    input: TypedPipe[(Long, (K, W))])(implicit flowDef: FlowDef, mode: Mode) =
    doJoin(input, readDateRange(covering))
}

trait SourceUniqueKeyedService[S <: SSource, K, V] extends UniqueKeyedService[K, V] {
  def source(dr: DateRange): S
  def toPipe(s: S)(implicit flow: FlowDef, mode: Mode): TypedPipe[(K,V)]
  def reducers: Option[Int]

  def satisfiable(requested: DateRange, mode: Mode): Try[DateRange] =
    Scalding.minify(mode, requested)(source(_))

  final override def readDateRange(req: DateRange)(implicit flowDef: FlowDef, mode: Mode) =
    toPipe(source(req))
}

object UniqueKeyedService extends java.io.Serializable {
  def from[K:Ordering,V](fn: DateRange => Mappable[(K,V)], reducers: Option[Int] = None): UniqueKeyedService[K, V] =
    fromAndThen[(K,V),K,V](fn, identity, reducers)

  /** The Mappable is the subclass of Source that knows about the file system. */
  def fromAndThen[T,K:Ordering,V](
    fn: DateRange => Mappable[T],
    andThen: TypedPipe[T] => TypedPipe[(K,V)],
    inputReducers: Option[Int] = None): UniqueKeyedService[K, V] =
    new SourceUniqueKeyedService[Mappable[T], K, V] {
      def ordering = Ordering[K]
      def source(dr: DateRange) = fn(dr)
      def toPipe(mappable: Mappable[T])(implicit flow: FlowDef, mode: Mode) =
        andThen(TypedPipe.from(mappable)(flow, mode)) // converter is removed in 0.9.0
      def reducers: Option[Int] = inputReducers
    }
}
