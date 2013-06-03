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
  /**
    * Begin from some base representation. An iterator for in-memory,
    * for example.
    */
  def source[P, T, S](s: S)(implicit ser: Serialization[P, T], timeOf: TimeExtractor[T]): Producer[P, T] =
    Source[P, T, S](s, ser, timeOf)

  implicit def toKeyed[P, K, V](producer: Producer[P, (K, V)]): KeyedProducer[P, K, V] =
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

  /**
    * TODOS:
    * - This needs to push through to a proper Monad, not just TO.
    * - Special-case T => Option[U] as a new type of node. These nodes
    *   can be optimized when attached to sources.
    */
  def flatMap[U](fn: T => TraversableOnce[U]): Producer[P, U] =
    this match {
      // formerFn had to produce T, even though we don't know what
      // its input type was.
      case FlatMappedProducer(former, formerFn) =>
        FlatMappedProducer[P, Any, U](former, (formerFn(_).flatMap(fn)))
      case other => FlatMappedProducer[P, T, U](other, fn)
    }
}

case class Source[P, T, S](source: S, serialization: Serialization[P, T], timeOf: TimeExtractor[T])
    extends Producer[P, T]

case class NamedProducer[P, T](producer: Producer[P, T], id: String) extends Producer[P, T]

case class FlatMappedProducer[P, T, U](producer: Producer[P, T], fn: T => TraversableOnce[U]) extends Producer[P, U]

case class MergedProducer[P, T](left: Producer[P, T], right: Producer[P, T]) extends Producer[P, T]

case class Summer[P, K, V](
  producer: KeyedProducer[P, K, V],
  store: Store[P, K, V],
  kSer: Serialization[P, K],
  vSer: Serialization[P, V],
  monoid: Monoid[V],
  batcher: Batcher) extends KeyedProducer[P, K, V]

trait KeyedProducer[P, K, V] extends Producer[P, (K, V)] {
  def leftJoin[RightV](service: Service[P, K, RightV]): KeyedProducer[P, K, (V, Option[RightV])] =
    LeftJoinedProducer(this, service)

  /**
    * TODO: This could return a KeyedProducer, and we could keep going
    * with flatMap, etc.
    */
  def sumByKey(store: Store[P, K, V])(
    implicit kSer: Serialization[P, K],
    vSer: Serialization[P, V],
    monoid: Monoid[V], // TODO: Semigroup?
    batcher: Batcher): Summer[P, K, V] = Summer(this, store, kSer, vSer, monoid, batcher)
}

case class IdentityKeyedProducer[P, K, V](producer: Producer[P, (K, V)]) extends KeyedProducer[P, K, V]

case class LeftJoinedProducer[P, K, V, JoinedV](left: KeyedProducer[P, K, V], joined: Service[P, K, JoinedV])
    extends KeyedProducer[P, K, (V, Option[JoinedV])]
