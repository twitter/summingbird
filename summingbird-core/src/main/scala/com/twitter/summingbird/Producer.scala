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

import com.twitter.algebird.Semigroup

object Producer {

  /**
   * return this and the recursively reachable nodes in depth first, left first order
   * Differs from transitiveDependencies in that this goes up both sides of an either
   * and it returns the input node.
   */
  def entireGraphOf[P <: Platform[P]](p: Producer[P, Any]): List[Producer[P, Any]] = {
    val above = graph.depthFirstOf(p)(parentsOf)
    p :: above
  }

  /**
   * The parents of the current node, possibly not its dependencies
   *  It differs from dependencies in that it will also return the predecessor of an AlsoProducer
   */
  def parentsOf[P <: Platform[P]](in: Producer[P, Any]): List[Producer[P, Any]] = {
    in match {
      case AlsoProducer(l, r) => List(l, r)
      case _ => dependenciesOf(in)
    }
  }

  /** Returns the first Summer of the provided list of Producers */
  def retrieveSummer[P <: Platform[P]](paths: List[Producer[P, _]]): Option[Summer[P, _, _]] =
    paths.collectFirst { case s: Summer[P, _, _] => s }

  /**
   * A producer DAG starts from sources.
   * The actual source type depend on the Platform.
   * For example: An iterator for in-memory.
   */
  def source[P <: Platform[P], T](s: P#Source[T]): Producer[P, T] = Source[P, T](s)

  /**
   * implicit conversion from Producer[P, T] to KeyedProducer[P, K, V] when the type T is a tuple of (K, V) as proven by evidence ev
   *  enabling the operations on keys (sumByKey, ...)
   */
  implicit def evToKeyed[P <: Platform[P], T, K, V](producer: Producer[P, T])(implicit ev: T <:< (K, V)): KeyedProducer[P, K, V] =
    IdentityKeyedProducer[P, K, V](producer.asInstanceOf[Producer[P, (K, V)]])

  /**
   * implicit conversion from Producer[P, (K, V)] to KeyedProducer[P, K, V]
   *  enabling the operations on keys (sumByKey, ...)
   */
  implicit def toKeyed[P <: Platform[P], K, V](producer: Producer[P, (K, V)]): KeyedProducer[P, K, V] =
    IdentityKeyedProducer[P, K, V](producer)

  /** a semigroup on producers where + means merge */
  implicit def semigroup[P <: Platform[P], T]: Semigroup[Producer[P, T]] =
    Semigroup.from(_ merge _)

  /** the list of the Producers, this producer directly depends on */
  def dependenciesOf[P <: Platform[P]](p: Producer[P, Any]): List[Producer[P, Any]] =
    p match {
      case Source(_) => List()
      case AlsoProducer(_, producer) => List(producer)
      case NamedProducer(producer, _) => List(producer)
      case IdentityKeyedProducer(producer) => List(producer)
      case OptionMappedProducer(producer, _) => List(producer)
      case FlatMappedProducer(producer, _) => List(producer)
      case KeyFlatMappedProducer(producer, _) => List(producer)
      case ValueFlatMappedProducer(producer, _) => List(producer)
      case WrittenProducer(producer, _) => List(producer)
      case LeftJoinedProducer(producer, _) => List(producer)
      case Summer(producer, _, _) => List(producer)
      case MergedProducer(l, r) => List(l, r)
    }

  /**
   * Returns true if this node does not directly change the data (does not apply any transformation)
   */
  def isNoOp[P <: Platform[P]](p: Producer[P, Any]): Boolean = p match {
    case IdentityKeyedProducer(_) => true
    case NamedProducer(_, _) => true
    case MergedProducer(_, _) => true
    case AlsoProducer(_, _) => true
    // The rest do something
    case Source(_) => false
    case OptionMappedProducer(_, _) => false
    case FlatMappedProducer(_, _) => false
    case KeyFlatMappedProducer(_, _) => false
    case ValueFlatMappedProducer(_, _) => false
    case WrittenProducer(_, _) => false
    case LeftJoinedProducer(_, _) => false
    case Summer(_, _, _) => false
  }

  /** returns true if this Producer is an output of the DAG (Summer and WrittenProducer) */
  def isOutput[P <: Platform[P]](p: Producer[P, Any]): Boolean = p match {
    case Summer(_, _, _) | WrittenProducer(_, _) => true
    case _ => false
  }

  /**
   * Return all dependencies of a given node in depth first, left first order.
   */
  def transitiveDependenciesOf[P <: Platform[P]](p: Producer[P, Any]): List[Producer[P, Any]] = {
    val nfn = dependenciesOf[P](_)
    graph.depthFirstOf(p)(nfn)
  }
}

/**
 * A Producer is a node in our tree, able to generate new items and
 * have operations applied to it. In Storm, this might be an
 * in-progress TopologyBuilder.
 */
sealed trait Producer[P <: Platform[P], +T] {

  /** Exactly the same as merge. Here by analogy with the scala.collections API */
  def ++[U >: T](r: Producer[P, U]): Producer[P, U] = MergedProducer(this, r)

  /**
   * Naming a node is so that you may give Options for that node that may change
   * the run-time performance of the job (parameter tuning, etc...)
   */
  def name(id: String): Producer[P, T] = NamedProducer(this, id)

  /** Combine the output into one Producer */
  def merge[U >: T](r: Producer[P, U]): Producer[P, U] = MergedProducer(this, r)

  /**
   * Prefer to flatMap for transforming a subset of items
   * like optionMap but convenient with case syntax in scala
   * prod.collect { case x if fn(x) => g(x) }
   */
  def collect[U](fn: PartialFunction[T, U]): Producer[P, U] =
    optionMap(fn.lift)

  /** Keep only the items that satisfy the fn */
  def filter(fn: T => Boolean): Producer[P, T] =
    // Enforce using the OptionMapped here:
    optionMap(Some(_).filter(fn))

  /**
   * This is identical to a certain leftJoin:
   * map((_, ())).leftJoin(srv).mapValues{case (_, v) => v}
   * Useful when you are looking up values from
   * say a stream of inputs, such as IDs.
   */
  def lookup[U >: T, V](service: P#Service[U, V]): KeyedProducer[P, U, Option[V]] =
    map[(U, Unit)]((_, ())).leftJoin(service).mapValues { case (_, v) => v }

  /** Map each item to a new value */
  def map[U](fn: T => U): Producer[P, U] =
    // Enforce using the OptionMapped here:
    optionMap(t => Some(fn(t)))

  /**
   * Prefer this or collect to flatMap if you are always emitting 0 or 1 items
   */
  def optionMap[U](fn: T => Option[U]): Producer[P, U] =
    OptionMappedProducer[P, T, U](this, fn)

  /**
   * Only use this function if you may return more than 1 item sometimes.
   * otherwise use collect or optionMap, which can be pushed up the graph
   */
  def flatMap[U](fn: T => TraversableOnce[U]): Producer[P, U] =
    FlatMappedProducer[P, T, U](this, fn)

  /**
   * Cause some side effect on the sink, but pass through the values so
   * they can be consumed downstream
   */
  def write[U >: T](sink: P#Sink[U]): TailProducer[P, T] = WrittenProducer(this, sink)

  /** Merge a different type of Producer into a single stream */
  def either[U](other: Producer[P, U]): Producer[P, Either[T, U]] =
    map(Left(_): Either[T, U])
      .merge(other.map(Right(_): Either[T, U]))
}

/** Wraps the sources of the given Platform */
case class Source[P <: Platform[P], T](source: P#Source[T])
  extends Producer[P, T]

/** Only TailProducers can be planned. There is nothing after a tail */
sealed trait TailProducer[P <: Platform[P], +T] extends Producer[P, T] {
  /**
   * Ensure this is scheduled, but return something equivalent to the argument
   * like the function `par` in Haskell.
   * This can be used to combine two independent Producers in a way that ensures
   * that the Platform will plan both into a single Plan.
   */
  def also[R](that: TailProducer[P, R])(implicit ev: DummyImplicit): TailProducer[P, R] =
    new AlsoTailProducer(this, that)

  def also[R](that: Producer[P, R]): Producer[P, R] = AlsoProducer(this, that)

  override def name(id: String): TailProducer[P, T] = new TPNamedProducer[P, T](this, id)
}

class AlsoTailProducer[P <: Platform[P], +T, +R](ensure: TailProducer[P, T], result: TailProducer[P, R]) extends AlsoProducer[P, T, R](ensure, result) with TailProducer[P, R]

/**
 * This is a special node that ensures that the first argument is planned, but produces values
 * equivalent to the result.
 */
case class AlsoProducer[P <: Platform[P], +T, +R](ensure: TailProducer[P, T], result: Producer[P, R]) extends Producer[P, R]

case class NamedProducer[P <: Platform[P], +T](producer: Producer[P, T], id: String) extends Producer[P, T]

class TPNamedProducer[P <: Platform[P], +T](producer: Producer[P, T], id: String) extends NamedProducer[P, T](producer, id) with TailProducer[P, T]

/**
 * Represents filters and maps which may be optimized differently
 * Note that "option-mapping" is closed under composition and hence useful to call out
 */
case class OptionMappedProducer[P <: Platform[P], T, +U](producer: Producer[P, T], fn: T => Option[U])
  extends Producer[P, U]

case class FlatMappedProducer[P <: Platform[P], T, +U](producer: Producer[P, T], fn: T => TraversableOnce[U])
  extends Producer[P, U]

case class MergedProducer[P <: Platform[P], +T](left: Producer[P, T], right: Producer[P, T]) extends Producer[P, T]

case class WrittenProducer[P <: Platform[P], T, U >: T](producer: Producer[P, T], sink: P#Sink[U]) extends TailProducer[P, T]

case class Summer[P <: Platform[P], K, V](
  producer: Producer[P, (K, V)],
  store: P#Store[K, V],
  semigroup: Semigroup[V]) extends KeyedProducer[P, K, (Option[V], V)] with TailProducer[P, (K, (Option[V], V))]

/**
 * This has the methods on Key-Value streams.
 * The rule is: if you can easily express your logic on the keys and values independently,
 * do it! This is how you communicate structure to Summingbird and it uses these hints
 * to attempt the most efficient run of your code.
 */
sealed trait KeyedProducer[P <: Platform[P], K, V] extends Producer[P, (K, V)] {

  /** Builds a new KeyedProvider by applying a partial function to keys of elements of this one on which the function is defined.*/
  def collectKeys[K2](pf: PartialFunction[K, K2]): KeyedProducer[P, K2, V] =
    IdentityKeyedProducer(collect { case (k, v) if pf.isDefinedAt(k) => (pf(k), v) })

  /** Builds a new KeyedProvider by applying a partial function to values of elements of this one on which the function is defined.*/
  def collectValues[V2](pf: PartialFunction[V, V2]): KeyedProducer[P, K, V2] =
    flatMapValues { v => if (pf.isDefinedAt(v)) Iterator(pf(v)) else Iterator.empty }

  /**
   * Prefer this to filter or flatMap/flatMapKeys if you are filtering.
   * This may be optimized in the future with an intrinsic node in the Producer graph.
   * We know this never increases the number of items, and we know it does not rekey
   * the partition.
   */
  def filterKeys(pred: K => Boolean): KeyedProducer[P, K, V] =
    IdentityKeyedProducer(filter { case (k, _) => pred(k) })

  /**
   * Prefer this to filter or flatMap/flatMapValues if you are filtering.
   * This may be optimized in the future with an intrinsic node in the Producer graph.
   * We know this never increases the number of items, and we know it does not rekey
   * the partition.
   */
  def filterValues(pred: V => Boolean): KeyedProducer[P, K, V] =
    flatMapValues { v => if (pred(v)) Iterator(v) else Iterator.empty }

  /**
   * Prefer to call this method to flatMap if you are expanding only keys.
   * It may trigger optimizations, that can significantly improve performance
   */
  def flatMapKeys[K2](fn: K => TraversableOnce[K2]): KeyedProducer[P, K2, V] =
    KeyFlatMappedProducer(this, fn)

  /** Prefer this to a raw map as this may be optimized to avoid a key reshuffle */
  def flatMapValues[U](fn: V => TraversableOnce[U]): KeyedProducer[P, K, U] =
    ValueFlatMappedProducer(this, fn)

  /** Return just the keys */
  def keys: Producer[P, K] = map(_._1)

  /**
   * Do a lookup/join on a service. This is how you trigger async computation is summingbird.
   * Any remote API call, DB lookup, etc... happens here
   */
  def leftJoin[RightV](service: P#Service[K, RightV]): KeyedProducer[P, K, (V, Option[RightV])] =
    LeftJoinedProducer(this, service)

  /**
   * Do a windowed join on a stream. You need to provide a sink that manages
   * the buffer. Offline, this might be a bounded HDFS partition. Online it
   * might be a cache that evicts after a period of time.
   */
  def leftJoin[RightV](stream: KeyedProducer[P, K, RightV],
    buffer: P#Buffer[K, RightV]): KeyedProducer[P, K, (V, Option[RightV])] =
    stream.write(buffer)
      .also(leftJoin(buffer))

  /**
   * Prefer to call this method to flatMap/map if you are mapping only keys.
   * It may trigger optimizations, that can significantly improve performance
   */
  def mapKeys[K2](fn: K => K2): KeyedProducer[P, K2, V] =
    KeyFlatMappedProducer(this, fn.andThen(Iterator(_)))

  /** Prefer this to a raw map as this may be optimized to avoid a key reshuffle */
  def mapValues[U](fn: V => U): KeyedProducer[P, K, U] =
    flatMapValues { v => Iterator(fn(v)) }

  /**
   * emits a KeyedProducer with a value that is the store value, just BEFORE a merge,
   * and the right is a new delta (which may include, depending on the Platform, Store and Options,
   * more than a single aggregated item).
   *
   * so, the sequence out of this has the property that:
   * (v0, vdelta1), (v0 + vdelta1, vdelta2), (v0 + vdelta1 + vdelta2, vdelta3), ...
   *
   */
  def sumByKey(store: P#Store[K, V])(implicit semigroup: Semigroup[V]): Summer[P, K, V] =
    Summer(this, store, semigroup)

  /** Exchange values for keys */
  def swap: KeyedProducer[P, V, K] = IdentityKeyedProducer(map(_.swap))

  /** Keep only the values */
  def values: Producer[P, V] = map(_._2)
}

case class KeyFlatMappedProducer[P <: Platform[P], K, V, K2](producer: Producer[P, (K, V)], fn: K => TraversableOnce[K2]) extends KeyedProducer[P, K2, V]

case class ValueFlatMappedProducer[P <: Platform[P], K, V, V2](producer: Producer[P, (K, V)],
  fn: V => TraversableOnce[V2]) extends KeyedProducer[P, K, V2]

case class IdentityKeyedProducer[P <: Platform[P], K, V](producer: Producer[P, (K, V)]) extends KeyedProducer[P, K, V]

case class LeftJoinedProducer[P <: Platform[P], K, V, JoinedV](left: Producer[P, (K, V)], joined: P#Service[K, JoinedV])
  extends KeyedProducer[P, K, (V, Option[JoinedV])]
