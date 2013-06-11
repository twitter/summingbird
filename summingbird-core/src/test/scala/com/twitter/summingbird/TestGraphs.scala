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

import com.twitter.algebird.MapAlgebra
import com.twitter.algebird.Monoid
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

/**
  * Helpful functions and test graphs designed to flex Summingbird
  * planners.
  */

object TestGraphs {
  // TODO: These functions were lifted from Storehaus's testing
  // suite. They should move into Algebird to make it easier to test
  // maps that have had their zeros removed with MapAlgebra.

  def rightContainsLeft[K,V: Equiv](l: Map[K, V], r: Map[K, V]): Boolean =
    l.foldLeft(true) { (acc, pair) =>
      acc && r.get(pair._1).map { Equiv[V].equiv(_, pair._2) }.getOrElse(true)
    }

  implicit def mapEquiv[K,V: Monoid: Equiv]: Equiv[Map[K, V]] = {
    Equiv.fromFunction { (m1, m2) =>
      val cleanM1 = MapAlgebra.removeZeros(m1)
      val cleanM2 = MapAlgebra.removeZeros(m2)
      rightContainsLeft(cleanM1, cleanM2) && rightContainsLeft(cleanM2, cleanM1)
    }
  }

  def singleStepJob[P <: Platform[P], T, K, V: Monoid](
    source: Producer[P, T],
    store: P#Store[K, V])(fn: T => TraversableOnce[(K, V)]): Summer[P, K, V] =
    source
      .flatMap(fn)
      .sumByKey(store)

    /**
    * Accepts a platform, and supplier of an EMPTY store from K -> V
    * and returns a ScalaCheck property. The property generates a
    * random function from T => TraversableOnce[(K, V)], wires up
    * singleStepJob summingbird job using this function and runs the
    * job using the supplied platform.
    *
    * Results are retrieved using the supplied toMap function. The
    * initial data source is generated using the supplied sourceMaker
    * function.
    */
  def singleStepChecker[P <: Platform[P], T: Manifest: Arbitrary, K: Arbitrary, V: Monoid: Arbitrary: Equiv]
    (platform: P, store: => P#Store[K, V])
    (sourceMaker: TraversableOnce[T] => Producer[P, T])
    (toMap: P#Store[K, V] => Map[K, V]) =
    forAll { (items: List[T], fn: T => List[(K, V)]) =>

      // Use the supplied platform to execute the source into the
      // supplied store.
      platform.run {
        singleStepJob(sourceMaker(items), store)(fn)
      }
      // Then, convert the store into a Map and compare to the result
      // of performing the equivalent operation in pure Scala.
      Equiv[Map[K, V]].equiv(
        toMap(store),
        MapAlgebra.sumByKey(items.flatMap(fn))
      )
    }

  def leftJoinJob[P <: Platform[P], T, U, JoinedU, K, V: Monoid](
    source: Producer[P, T],
    service: P#Service[K, JoinedU],
    store: P#Store[K, V])
    (preJoinFn: T => TraversableOnce[(K, U)])
    (postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): Summer[P, K, V] =
    source
      .flatMap(preJoinFn)
      .leftJoin(service)
      .flatMap(postJoinFn)
      .sumByKey(store)

  /**
    * Accepts a platform, a service of K -> JoinedU and a supplier of
    * an EMPTY store from K -> V and returns a ScalaCheck
    * property. The property generates random functions between the
    * types required by leftJoinJob, wires up a summingbird job using
    * those functions and runs the job using the supplied platform.
    *
    * Results are retrieved using the supplied toMap function. The
    * service is tested in scala's in-memory mode using serviceToFn,
    * and the initial data source is generated using the supplied
    * sourceMaker function.
    */
  def leftJoinChecker[P <: Platform[P], T: Manifest: Arbitrary, K: Arbitrary, U: Arbitrary, JoinedU: Arbitrary, V: Monoid: Arbitrary: Equiv]
    (platform: P, service: P#Service[K, JoinedU], store: => P#Store[K, V])
    (sourceMaker: TraversableOnce[T] => Producer[P, T])
    (serviceToFn: P#Service[K, JoinedU] => (K => Option[JoinedU]))
    (toMap: P#Store[K, V] => Map[K, V]) =
    forAll { (items: List[T], preJoinFn: T => List[(K, U)], postJoinFn: ((K, (U, Option[JoinedU]))) => List[(K, V)]) =>
      platform.run {
        leftJoinJob(sourceMaker(items), service, store)(preJoinFn)(postJoinFn)
      }
      val serviceFn = serviceToFn(service)
      Equiv[Map[K, V]].equiv(
        toMap(store),
        MapAlgebra.sumByKey {
          items
            .flatMap(preJoinFn)
            .map { case (k, u) => (k, (u, serviceFn(k))) }
            .flatMap(postJoinFn)
        }
      )
    }
}
