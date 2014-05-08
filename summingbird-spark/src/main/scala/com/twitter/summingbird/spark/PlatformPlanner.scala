package com.twitter.summingbird.spark

import com.twitter.summingbird._
import com.twitter.algebird.Semigroup
import scala.reflect.ClassTag

/**
 * The DAG handed to a platform by summingbird is a little hard to work with
 * given that all the generic types have been erased. This helper gives you
 * type safe methods to implement, and casts / coerces things for you.
 *
 * TODO: Is this useful outside of the spark platform? Is it useful at all?
 *
 * @author Alex Levenson
 */
trait PlatformPlanner[P <: Platform[P]] {

  // some shorthand type aliases
  type Prod[T] =  Producer[P, T]
  type TailProd[T] =  TailProducer[P, T]
  type Visited = Map[Prod[_], P#Plan[_]]

  case class PlanState[T](
    plan: P#Plan[T],
    visited: Visited
  )

  def visit[T](producer: Prod[T]) = toPlan(producer, Map.empty)

  protected def toPlan[T](producer: Prod[T], visited:  Visited): PlanState[T] = {
    // short circuit if we've already visited this node
    visited.get(producer) match {
      case Some(s) => PlanState(s.asInstanceOf[P#Plan[T]], visited)
      case None => toPlan2(producer, visited)
    }
  }

  private def toPlan2[T](outer: Prod[T], visited: Visited): PlanState[T] = {

    val updatedState = outer match {
      case NamedProducer(prod, name) => planNamedProducer(prod, name, visited)

      case IdentityKeyedProducer(prod) => planIdentityKeyedProducer(prod, visited)

      case Source(source) => planSource(source, visited)

      case OptionMappedProducer(prod, fn) => planOptionMappedProducer[Any, Any](prod, visited, fn)

      case FlatMappedProducer(prod, fn) => planFlatMappedProducer[Any, Any](prod, visited, fn)

      case MergedProducer(l, r) => planMergedProducer(l, r, visited)

      case KeyFlatMappedProducer(prod, fn) => planKeyFlatMappedProducer(prod, visited, fn)

      case AlsoProducer(ensure, result) => planAlsoProducer[Any, Any](ensure, result, visited)

      case WrittenProducer(prod, sink) => planWrittenProducer(prod, visited, sink)

      case LeftJoinedProducer(prod, service) => planLeftJoinedProducer(prod, visited, service)

      case summer@Summer(prod, store, semigroup) => planSummer(summer, prod, visited, store, semigroup)
    }

    PlanState(updatedState.plan.asInstanceOf[P#Plan[T]], updatedState.visited + (outer -> updatedState.plan))
  }

  def planNamedProducer[T](prod: Prod[T], name: String, visited: Visited): PlanState[T]

  def planIdentityKeyedProducer[K, V](prod: Prod[(K, V)], visited: Visited): PlanState[(K, V)]

  def planSource[T](source: P#Source[T], visited: Visited): PlanState[T]

  def planOptionMappedProducer[T, U: ClassTag](prod: Prod[T], visited: Visited, fn: (T) => Option[U]): PlanState[U]

  def planFlatMappedProducer[T, U: ClassTag](prod: Prod[T], visited: Visited, fn: (T) => TraversableOnce[U]): PlanState[U]

  def planMergedProducer[T](l: Prod[T], r: Prod[T], visited: Visited): PlanState[T]

  def planKeyFlatMappedProducer[K, V, K2](prod: Prod[(K, V)], visited: Visited, fn: K => TraversableOnce[K2]): PlanState[(K2, V)]

  def planAlsoProducer[E, R: ClassTag](ensure: TailProd[E], result: Prod[R], visited: Visited): PlanState[R]

  def planWrittenProducer[T: ClassTag](prod: Prod[T], visited: Visited, sink: P#Sink[T]): PlanState[T]

  def planLeftJoinedProducer[K: ClassTag, V: ClassTag, JoinedV](prod: Prod[(K, V)], visited: Visited, service: P#Service[K, JoinedV]): PlanState[(K, (V, Option[JoinedV]))]

  def planSummer[K: ClassTag, V: ClassTag](summer: Summer[P, K, V], prod: Prod[(K, V)], visited: Visited, store: P#Store[K, V], semigroup: Semigroup[V]): PlanState[(K, (Option[V], V))]

}
