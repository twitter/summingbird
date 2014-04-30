package com.twitter.summingbird.spark

import org.apache.spark.SparkContext._
import com.twitter.algebird.Monoid
import com.twitter.summingbird._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * This is a first pass at an offline [[Platform]] that executes on apache spark.
 *
 * TODO: Add in batching!
 * TODO: This is probably something that the Store will do
 *
 * @author Alex Levenson
 */
class SparkPlatform
    extends Platform[SparkPlatform] with PlatformPlanner[SparkPlatform] {

  // TODO: make this SparkContext => RDD[T] instead of RDD[T]

  // source is just an RDD // this needs a Time associated with it
  override type Source[T] = RDD[T]

  // store is a map of key to value, represented as an RDD of pairs
  // it is assumed that this RDD has no duplicate keys
  // TODO: is that a valid assumption?
  override type Store[K, V] = RDD[(K, V)]

  // sink is just a function from RDD => Unit
  // its only used for causing side effects
  override type Sink[T] = SparkSink[T]

  // service is just a keyed RDD
  override type Service[K, V] = RDD[(K, V)]
  override type Plan[T] = RDD[T]

  private[this] val writeClosures: ArrayBuffer[() => Unit] = ArrayBuffer()

  override def plan[T](completed: TailProducer[SparkPlatform, T]): SparkPlatform#Plan[T] = {
    visit(completed).plan
  }

  override def planNamedProducer[T](prod: Prod[T], visited: Visited): PlanState[T] = {
    toPlan(prod, visited)
  }

  override def planIdentityKeyedProducer[K, V](prod: Prod[(K, V)], visited: Visited): PlanState[(K, V)] = {
    toPlan(prod, visited)
  }

  override def planSource[T](source: Source[T], visited: Visited): PlanState[T] = {
    PlanState(source, visited)
  }

  override def planOptionMappedProducer[T, U: ClassTag](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => Option[U]): PlanState[U]  = {

    planFlatMappedProducer[T, U](prod, visited, x => fn(x))
  }

  override def planFlatMappedProducer[T, U: ClassTag](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => TraversableOnce[U]): PlanState[U] = {

    val planState = toPlan(prod, visited)
    PlanState(planState.plan.flatMap(fn(_)), planState.visited)
  }

  override def planMergedProducer[T](left: Prod[T], right: Prod[T], visited: Visited): PlanState[T] = {
    // plan down both sides of the tree
    val leftPlanState = toPlan(left, visited)
    val rightPlanState = toPlan(right, leftPlanState.visited)

    // when those are both done, union the results
    val both = leftPlanState.plan ++ rightPlanState.plan
    PlanState(both, rightPlanState.visited)
  }

  override def planKeyFlatMappedProducer[K, V, K2](
    prod: Prod[(K, V)],
    visited: Visited,
    fn: K => TraversableOnce[K2]): PlanState[(K2, V)] = {
      val planState = toPlan(prod, visited)
      val mapped = planState.plan.flatMap {
        case (key, value) => fn(key).map { (_, value) }
      }

    PlanState(mapped, planState.visited)
  }

  override def planAlsoProducer[E, R: ClassTag](ensure: TailProd[E], result: Prod[R], visited: Visited): PlanState[R] = {
    val ensurePlanState = toPlan(ensure, visited)
    val resultPlanState = toPlan(result, ensurePlanState.visited)
    resultPlanState
  }

  override def planWrittenProducer[T: ClassTag](prod: Prod[T], visited: Visited, sink: Sink[T]): PlanState[T]  = {
    val planState = toPlan(prod, visited)
    writeClosures += { () =>
      sink.write(planState.plan)
    }
    planState
  }

  override def planLeftJoinedProducer[K: ClassTag, V: ClassTag, JoinedV](prod: Prod[(K, V)], visited: Visited, service: Service[K, JoinedV]):
    PlanState[(K, (V, Option[JoinedV]))] = {

    val planState = toPlan(prod, visited)

    val joined = planState.plan.leftOuterJoin(service)

    PlanState(joined, planState.visited)
  }

  // TODO: whose job is it to write the result of a sum?
  override def planSummer[K: ClassTag, V: ClassTag](
    prod: Prod[(K, V)],
    visited: Visited,
    store: Store[K, V],
    monoid: Monoid[V]): PlanState[(K, (Option[V], V))] = {

    // Assumptions:
    // producer has many duplicate keys
    // store has no duplicate keys

    val planState = toPlan(prod, visited)

    // first sum all the deltas
    // TODO: is there a way to use sumOption in map-side aggregation?
    // TODO: this assumes commutivity
    // TODO: need to check that first
    // TODO: non commutative needs a sort on time etc.
    val summedDeltas = planState.plan.reduceByKey { (x, y) => monoid.plus(x, y) }

    // join deltas with stored values
    val summedDeltasWithStoredValues = summedDeltas.leftOuterJoin(store)

    val summed = summedDeltasWithStoredValues.map {
      case (key, (deltaSum, storedValue)) => {
        val sum = storedValue.map { s =>  monoid.plus(s, deltaSum) }.getOrElse(deltaSum)
        (key, (storedValue, sum))
      }
    }

    PlanState(summed, planState.visited)
  }

  def run() = {
    // TODO: thread pool
    writeClosures.foreach { c => c.apply() }
  }

}