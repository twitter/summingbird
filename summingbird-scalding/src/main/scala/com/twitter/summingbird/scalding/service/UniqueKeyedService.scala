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

package com.twitter.summingbird.scalding.service

import com.twitter.summingbird.scalding._
import com.twitter.summingbird.batch.Timestamp
import com.twitter.scalding.{ Grouped => _, Source => SSource, _ }
import com.twitter.scalding.typed.{ TypedPipe => _, _ }
import cascading.flow.FlowDef
import scala.util.control.NonFatal

/**
 * A UniqueKeyedService covers the case where Keys are globally
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
  def doJoin[W](in: TypedPipe[(Timestamp, (K, W))],
    serv: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[(Timestamp, (K, (W, Option[V])))] = {
    implicit val ord: Ordering[K] = ordering
    def withReducers[U, T](grouped: Grouped[U, T]) =
      reducers.map { grouped.withReducers(_) }.getOrElse(grouped)

    withReducers(in.map { case (t, (k, w)) => (k, (t, w)) }.group)
      .leftJoin(withReducers(serv.group))
      .toTypedPipe
      .map { case (k, ((t, w), optV)) => (t, (k, (w, optV))) }
  }

  final override def serve[W](covering: DateRange,
    input: TypedPipe[(Timestamp, (K, W))])(implicit flowDef: FlowDef, mode: Mode) =
    doJoin(input, readDateRange(covering))
}

trait SourceUniqueKeyedService[S <: SSource, K, V] extends UniqueKeyedService[K, V] {
  def source(dr: DateRange): S
  def toPipe(s: S)(implicit flow: FlowDef, mode: Mode): TypedPipe[(K, V)]
  def reducers: Option[Int]

  def satisfiable(requested: DateRange, mode: Mode): Try[DateRange] =
    Scalding.minify(mode, requested)(source(_))

  final override def readDateRange(req: DateRange)(implicit flowDef: FlowDef, mode: Mode) =
    toPipe(source(req))
}

object UniqueKeyedService extends java.io.Serializable {

  def from[K: Ordering, V](fn: DateRange => Mappable[(K, V)],
    reducers: Option[Int] = None,
    requireFullySatisfiable: Boolean = false): UniqueKeyedService[K, V] =
    fromAndThen[(K, V), K, V](fn, identity, reducers, requireFullySatisfiable)

  /** The Mappable is the subclass of Source that knows about the file system. */
  def fromAndThen[T, K: Ordering, V](
    fn: DateRange => Mappable[T],
    andThen: TypedPipe[T] => TypedPipe[(K, V)],
    inputReducers: Option[Int] = None,
    requireFullySatisfiable: Boolean = false): UniqueKeyedService[K, V] =
    new SourceUniqueKeyedService[Mappable[T], K, V] {
      def ordering = Ordering[K]
      def source(dr: DateRange) = fn(dr)
      def toPipe(mappable: Mappable[T])(implicit flow: FlowDef, mode: Mode) =
        andThen(TypedPipe.from(mappable))

      def reducers: Option[Int] = inputReducers

      override def satisfiable(requested: DateRange, mode: Mode): Try[DateRange] = {
        if (requireFullySatisfiable) {
          val s = fn(requested)
          try {
            s.validateTaps(mode)
            Right(requested)
          } catch {
            case NonFatal(e) =>
              val msg = "Requested range: %s for source %s was not fully satisfiable.\n".format(requested, s)
              toTry(e, msg)
          }
        } else {
          super.satisfiable(requested, mode)
        }
      }
    }
}
