package com.twitter.summingbird.spark

import com.twitter.algebird.Monoid
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.summingbird._
import com.twitter.util.{FuturePool, Future}
import java.util.concurrent._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.reflect.ClassTag

/**
 * This is a first pass at an offline [[Platform]] that executes on apache spark.
 *
 * TODO: Add in batching!
 *
 * @author Alex Levenson
 */
class SparkPlatform
    extends Platform[SparkPlatform] with PlatformPlanner[SparkPlatform] {

  // TODO: Oscar mentioned making these SparkContext => RDD[T] instead of RDD[T]
  //      but what's the benefit?

  // source is just an RDD
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
  override type Plan[T] = Future[RDD[T]]

  // spark 'actions' block the current thread and start doing work immediately.
  // as we build up a plan for spark, we want to be able to queue up 'actions' without
  // really triggering work on the cluster until all the planning is done. So we submit
  // this actions to a special FuturePool that doesn't start doing work until start() is called.
  private val pool = new WaitingFuturePool(
    FuturePool(Executors.newCachedThreadPool( // unbounded thread pool
      new NamedPoolThreadFactory("SparkPlatformPool", true)))) // use daemon threads

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
    PlanState(Future.value(source), visited)
  }

  override def planOptionMappedProducer[T, U: ClassTag](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => Option[U]): PlanState[U]  = {

    planFlatMappedProducer(prod, visited, x => fn(x))
  }

  override def planFlatMappedProducer[T, U: ClassTag](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => TraversableOnce[U]): PlanState[U] = {

    val planState = toPlan(prod, visited)
    PlanState(planState.plan.map { rdd => rdd.flatMap(fn(_)) }, planState.visited)
  }

  override def planMergedProducer[T](left: Prod[T], right: Prod[T], visited: Visited): PlanState[T] = {
    // plan down both sides of the tree
    val leftPlanState = toPlan(left, visited)
    val rightPlanState = toPlan(right, leftPlanState.visited)

    // when those are both done, union the results
    val both = Future.join(leftPlanState.plan, rightPlanState.plan)
    PlanState(both.map { case (l, r) => l ++ r }, rightPlanState.visited)
  }

  override def planKeyFlatMappedProducer[K, V, K2](
    prod: Prod[(K, V)],
    visited: Visited,
    fn: K => TraversableOnce[K2]): PlanState[(K2, V)] = {
      val planState = toPlan(prod, visited)
      val mapped = planState.plan.map { rdd => rdd.flatMap {
        case (key, value) => fn(key).map { (_, value) }
      }
    }

    PlanState(mapped, planState.visited)
  }

  override def planAlsoProducer[E, R: ClassTag](ensure: TailProd[E], result: Prod[R], visited: Visited): PlanState[R] = {
    val ensurePlanState = toPlan(ensure, visited)
    val resultPlanState = toPlan(result, ensurePlanState.visited)

    // QUESTION
    // better way to force execution?
    // does order of execution matter?
    // TODO: these currently happen in parallel, should they happen in sequence?
    val e = ensurePlanState.plan.flatMap {
      rdd => pool { rdd.foreach(x => Unit) }
    }

    val ret = Future.join(e, resultPlanState.plan).map { case (_, x) => x }

    PlanState(ret, resultPlanState.visited)
  }

  override def planWrittenProducer[T: ClassTag](prod: Prod[T], visited: Visited, sink: Sink[T]): PlanState[T]  = {
    val planState = toPlan(prod, visited)

    val written = planState.plan.flatMap {
      rdd => pool { sink.write(rdd) }
    }

    // QUESTION
    // does order of execution matter?
    // TODO: these currently happen in parallel, should they happen in sequence?
    val ret = Future.join(written, planState.plan).map { case (_, x) => x }

    PlanState(ret, planState.visited)
  }

  override def planLeftJoinedProducer[K: ClassTag, V: ClassTag, JoinedV](prod: Prod[(K, V)], visited: Visited, service: Service[K, JoinedV]):
    PlanState[(K, (V, Option[JoinedV]))] = {

    val planState = toPlan(prod, visited)

    val joined = planState.plan.map { rdd =>
      val pair: PairRDDFunctions[K, V] = rdd
      pair.leftOuterJoin(service)
    }

    PlanState(joined, planState.visited)
  }

  override def planSummer[K: ClassTag, V: ClassTag](
    prod: Prod[(K, V)],
    visited: Visited,
    store: Store[K, V],
    monoid: Monoid[V]): PlanState[(K, (Option[V], V))] = {

    val planState = toPlan(prod, visited)

    // Assumptions:
    // producer has many duplicate keys
    // store has no duplicate keys
    val summed = planState.plan.map { rdd =>

      // first sum all the deltas
      // TODO: is there a way to use sumOption in map-side aggregation?
      val summedDeltas = rdd.reduceByKey { (x, y) => monoid.plus(x, y) }

      // join deltas with stored values
      val summedDeltasWithStoredValues = summedDeltas.leftOuterJoin(store)

      summedDeltasWithStoredValues.map {
        case (key, (deltaSum, storedValue)) => {
          val sum = storedValue.map { s =>  monoid.plus(s, deltaSum) }.getOrElse(deltaSum)
          (key, (storedValue, sum))
        }
      }
    }

    PlanState(summed, planState.visited)
  }

  def run() = pool.start()

}