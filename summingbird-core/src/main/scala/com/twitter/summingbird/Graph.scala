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

/** Monoid stands alone. */
import com.twitter.algebird.Monoid
import com.twitter.summingbird.batch.Batcher

object Producer {
  def retrieveSummer[P <: Platform[P]](paths: List[Producer[P, _]]): Option[Summer[P, _, _]] =
    paths.collectFirst { case s: Summer[P, _, _] => s }

  /**
    * Begin from some base representation. An iterator for in-memory,
    * for example.
    */
  def source[P <: Platform[P], T](s: P#Source[T])(implicit manifest: Manifest[T], timeOf: TimeExtractor[T]): Producer[P, T] =
    Source[P, T](s, manifest, timeOf)

  implicit def toKeyed[P <: Platform[P], K, V](producer: Producer[P, (K, V)]): KeyedProducer[P, K, V] =
    IdentityKeyedProducer[P, K, V](producer)
}

/**
  * A Producer is a node in our tree, able to generate new items and
  * have operations applied to it. In Storm, this might be an
  * in-progress TopologyBuilder.
  */
sealed trait Producer[P, T] {
  def name(id: String): Producer[P, T] = NamedProducer(this, id)
  def merge(r: Producer[P, T]): Producer[P, T] = MergedProducer(this, r)

  def filter(fn: T => Boolean)(implicit mf: Manifest[T]): Producer[P, T] =
    // Enforce using the OptionMapped here:
    flatMap[T, Option[T]] { Some(_).filter(fn) }

  def map[U](fn: T => U)(implicit mf: Manifest[U]): Producer[P, U] =
    // Enforce using the OptionMapped here:
    flatMap[U, Some[U]] { t => Some(fn(t)) }

  def flatMap[U](fn: T => TraversableOnce[U]): Producer[P, U] =
    this match {
      // formerFn had to produce T, even though we don't know what
      // its input type was.
      case FlatMappedProducer(former, formerFn) =>
        FlatMappedProducer[P, Any, U](former, (formerFn(_).flatMap(fn)))
      case other => FlatMappedProducer[P, T, U](other, fn)
    }

  def flatMap[U, O](fn: T => O)(implicit ev: O <:< Option[U], manifest: Manifest[U]): Producer[P, U] = {
    // ev has not been serializable in the past, casting is safe:
    val optFn = fn.asInstanceOf[T => Option[U]]
    this match {
      case OptionMappedProducer(former, formerFn, _) =>
        OptionMappedProducer[P, Any, U](former, (formerFn(_).flatMap(optFn)), manifest)
      // If we have already flatMapped to Traversable, compose with that:
      case FlatMappedProducer(former, formerFn) =>
        FlatMappedProducer[P, Any, U](former,
          (formerFn(_).flatMap(optFn andThen Option.option2Iterable)))
      case other => OptionMappedProducer[P, T, U](other, optFn, manifest)
    }
  }
}

case class Source[P <: Platform[P], T](source: P#Source[T], manifest: Manifest[T], timeOf: TimeExtractor[T])
    extends Producer[P, T]

case class NamedProducer[P, T](producer: Producer[P, T], id: String) extends Producer[P, T]

/** Represents filters and maps which may be optimized differently
 * Note that "option-mapping" is closed under composition and hence useful to call out
 */
case class OptionMappedProducer[P, T, U](producer: Producer[P, T], fn: T => Option[U], manifest: Manifest[U]) extends Producer[P, U]

case class FlatMappedProducer[P, T, U](producer: Producer[P, T], fn: T => TraversableOnce[U]) extends Producer[P, U]

case class MergedProducer[P, T](left: Producer[P, T], right: Producer[P, T]) extends Producer[P, T]

case class Summer[P <: Platform[P], K, V](
  producer: KeyedProducer[P, K, V],
  store: P#Store[K, V],
  monoid: Monoid[V],
  batcher: Batcher) extends KeyedProducer[P, K, V]

sealed trait KeyedProducer[P <: Platform[P], K, V] extends Producer[P, (K, V)] {
  def leftJoin[RightV](service: P#Service[K, RightV]): KeyedProducer[P, K, (V, Option[RightV])] =
    LeftJoinedProducer(this, service)

  /**
    * TODO: This could return a KeyedProducer, and we could keep going
    * with flatMap, etc.
    */
  def sumByKey(store: P#Store[K, V])(
    implicit monoid: Monoid[V], // TODO: Semigroup?
    batcher: Batcher): Summer[P, K, V] = Summer(this, store, monoid, batcher)
}

case class IdentityKeyedProducer[P <: Platform[P], K, V](producer: Producer[P, (K, V)]) extends KeyedProducer[P, K, V]

case class LeftJoinedProducer[P <: Platform[P], K, V, JoinedV](left: KeyedProducer[P, K, V], joined: P#Service[K, JoinedV])
    extends KeyedProducer[P, K, (V, Option[JoinedV])]
