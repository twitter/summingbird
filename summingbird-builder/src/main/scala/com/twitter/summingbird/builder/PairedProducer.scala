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

package com.twitter.summingbird.builder

import com.twitter.algebird.Monoid
import com.twitter.summingbird.{ Producer, Platform }
import java.io.Serializable

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

case class PairedProducer[T, L <: Platform[L], R <: Platform[R]](l: Producer[L, T], r: Producer[R, T]) {
  def map[U: Manifest](fn: T => U) = PairedProducer(l.map(fn), r.map(fn))
  def filter(fn: T => Boolean)(implicit mf: Manifest[T]) = PairedProducer(l.filter(fn), r.filter(fn))
  def flatMap[U: Manifest](fn: T => TraversableOnce[U]) = PairedProducer(l.flatMap(fn), r.flatMap(fn))
  def leftJoin[K, V, JoinedV](leftService: L#Service[K, JoinedV], rightService: R#Service[K, JoinedV])
    (implicit ev: T <:< (K, V)) =
    PairedProducer(
      l.asInstanceOf[Producer[L, (K, V)]].leftJoin(leftService),
      r.asInstanceOf[Producer[R, (K, V)]].leftJoin(rightService)
    )
  def sumByKey[K, V](leftStore: L#Store[K, V], rightStore: R#Store[K, V])
    (implicit ev: T <:< (K, V), monoid: Monoid[V]) =
    PairedProducer(
      l.asInstanceOf[Producer[L, (K, V)]].sumByKey(leftStore),
      r.asInstanceOf[Producer[R, (K, V)]].sumByKey(rightStore)
    )
  def merge(other: PairedProducer[T, L, R]) = PairedProducer(l.merge(other.l), r.merge(other.r))
}
