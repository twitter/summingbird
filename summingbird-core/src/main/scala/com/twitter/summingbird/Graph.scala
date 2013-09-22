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
import com.twitter.algebird.{ Monoid, Semigroup }

object Producer {

  /** return this and the recursively reachable nodes in depth first, left first order
   * a tail is a node that has 0 dependencies in the Producer graph.
   * Either there are zero AlsoProducers, or this returns a List of length greater than 1.
   */
  def entireGraphOf[P <: Platform[P], T](p: Producer[P, T]): List[Producer[P, _]] = {
    // TODO: optimize and share such graph walking code in one place on generic types
    val parents = p match {
      case AlsoProducer(l, r) => List(l, r) // the left is not a dep, need a special case
      case _ => dependenciesOf(p)
    }
    val above = parents.flatMap(entireGraphOf(_))
    (p :: above).distinct
  }

  def retrieveSummer[P <: Platform[P]](paths: List[Producer[P, _]]): Option[Summer[P, _, _]] =
    paths.collectFirst { case s: Summer[P, _, _] => s }

  /**
    * Begin from some base representation. An iterator for in-memory,
    * for example.
    */
  def source[P <: Platform[P], T](s: P#Source[T])(implicit manifest: Manifest[T]): Producer[P, T] =
    Source[P, T](s, manifest)

  implicit def evToKeyed[P <: Platform[P], T, K, V](producer: Producer[P, T])
    (implicit ev: T <:< (K, V)): KeyedProducer[P, K, V] =
    IdentityKeyedProducer[P, K, V](producer.asInstanceOf[Producer[P, (K, V)]])

  implicit def toKeyed[P <: Platform[P], K, V](producer: Producer[P, (K, V)]): KeyedProducer[P, K, V] =
    IdentityKeyedProducer[P, K, V](producer)

  implicit def semigroup[P <: Platform[P], T]: Semigroup[Producer[P, T]] =
    Semigroup.from(_ merge _)

  def dependenciesOf[P <: Platform[P]](p: Producer[P, _]): List[Producer[P, _]] = {
    /*
     * Keyed producers seem to have some issue with type inference that
     * I work around with the cast.
     */
    p match {
      case AlsoProducer(_, prod) => List(prod)
      case NamedProducer(producer, _) => List(producer)
      case IdentityKeyedProducer(producer) => List(producer.asInstanceOf[Producer[P, _]])
      case Source(source, _) => List()
      case OptionMappedProducer(producer, fn, mf) => List(producer)
      case FlatMappedProducer(producer, fn) => List(producer)
      case MergedProducer(l, r) => List(l, r)
      case WrittenProducer(producer, fn) => List(producer)
      case LeftJoinedProducer(producer, service) => List(producer.asInstanceOf[Producer[P, _]])
      case Summer(producer, store, monoid) => List(producer.asInstanceOf[Producer[P, _]])
    }
  }

  /**
   * Return all dependencies of a given node in depth first, left first order.
   */
  def transitiveDependenciesOf[P <: Platform[P]](p: Producer[P, _]): List[Producer[P, _]] = {
    @annotation.tailrec
    def loop(stack: List[Producer[P, _]], deps: List[Producer[P,_]], acc: Set[Producer[P, _]]): (List[Producer[P,_]], Set[Producer[P, _]]) = {
      stack match {
        case Nil => (deps, acc)
        case h::tail =>
          val newStack = dependenciesOf(h).filterNot(acc).foldLeft(tail) { (s, it) => it :: s }
          val newDeps = if(acc.contains(h)) deps else (h :: deps)
          loop(newStack, newDeps, acc + h)
      }
    }
    val start = dependenciesOf(p)
    val (deps, _) = loop(start, start, start.toSet)
    deps
  }
}

/**
  * A Producer is a node in our tree, able to generate new items and
  * have operations applied to it. In Storm, this might be an
  * in-progress TopologyBuilder.
  */
sealed trait Producer[P <: Platform[P], T] {
  /** Ensure this is scheduled, but return something equivalent to the argument
   * like the function `par` in Haskell.
   * This can be used to combine two independent Producers in a way that ensures
   * that the Platform will plan both into a single Plan.
   */
  def also[R](that: Producer[P, R]): Producer[P, R] = AlsoProducer(this, that)
  def name(id: String): Producer[P, T] = NamedProducer(this, id)
  def merge(r: Producer[P, T]): Producer[P, T] = MergedProducer(this, r)

  def filter(fn: T => Boolean)(implicit mf: Manifest[T]): Producer[P, T] =
    // Enforce using the OptionMapped here:
    optionMap(Some(_).filter(fn))

  def map[U](fn: T => U)(implicit mf: Manifest[U]): Producer[P, U] =
    // Enforce using the OptionMapped here:
    optionMap(t => Some(fn(t)))

  def optionMap[U: Manifest](fn: T => Option[U]): Producer[P, U] =
    OptionMappedProducer[P, T, U](this, fn, manifest[U])

  def flatMap[U](fn: T => TraversableOnce[U]): Producer[P, U] =
    FlatMappedProducer[P, T, U](this, fn)

  def write(sink: P#Sink[T]): Producer[P, T] = WrittenProducer(this, sink)

  def either[U](other: Producer[P, U])(implicit tmf: Manifest[T], umf: Manifest[U]): Producer[P, Either[T, U]] =
    map(Left(_): Either[T, U])
      .merge(other.map(Right(_): Either[T, U]))
}

case class Source[P <: Platform[P], T](source: P#Source[T], manifest: Manifest[T])
    extends Producer[P, T]

/**
 * This is a special node that ensures that the first argument is planned, but produces values
 * equivalent to the result.
 */
case class AlsoProducer[P <: Platform[P], T, R](ensure: Producer[P, T], result: Producer[P, R]) extends Producer[P, R]

case class NamedProducer[P <: Platform[P], T](producer: Producer[P, T], id: String) extends Producer[P, T]

/** Represents filters and maps which may be optimized differently
 * Note that "option-mapping" is closed under composition and hence useful to call out
 */
case class OptionMappedProducer[P <: Platform[P], T, U](producer: Producer[P, T], fn: T => Option[U], manifest: Manifest[U])
    extends Producer[P, U]

case class FlatMappedProducer[P <: Platform[P], T, U](producer: Producer[P, T], fn: T => TraversableOnce[U])
    extends Producer[P, U]

case class MergedProducer[P <: Platform[P], T](left: Producer[P, T], right: Producer[P, T]) extends Producer[P, T]

case class WrittenProducer[P <: Platform[P], T](producer: Producer[P, T], sink: P#Sink[T]) extends Producer[P, T]

case class Summer[P <: Platform[P], K, V](
  producer: KeyedProducer[P, K, V],
  store: P#Store[K, V],
  monoid: Monoid[V]) extends KeyedProducer[P, K, V]

sealed trait KeyedProducer[P <: Platform[P], K, V] extends Producer[P, (K, V)] {
  def leftJoin[RightV](service: P#Service[K, RightV]): KeyedProducer[P, K, (V, Option[RightV])] =
    LeftJoinedProducer(this, service)

  def sumByKey(store: P#Store[K, V])(implicit monoid: Monoid[V]): Summer[P, K, V] =
    Summer(this, store, monoid)
}

case class IdentityKeyedProducer[P <: Platform[P], K, V](producer: Producer[P, (K, V)]) extends KeyedProducer[P, K, V]

case class LeftJoinedProducer[P <: Platform[P], K, V, JoinedV](left: KeyedProducer[P, K, V], joined: P#Service[K, JoinedV])
    extends KeyedProducer[P, K, (V, Option[JoinedV])]
