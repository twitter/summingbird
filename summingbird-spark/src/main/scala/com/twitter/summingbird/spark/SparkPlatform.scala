package com.twitter.summingbird.spark

import com.twitter.algebird.{Interval, Monoid}
import com.twitter.summingbird._
import com.twitter.summingbird.batch.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import com.twitter.summingbird.option.{NonCommutative, Commutative}
import com.twitter.chill.Externalizer

/**
 * This is a first pass at an offline [[Platform]] that executes on apache spark.
 *
 * TODO: add the logic for seeing what time spans each source / store / service can cover
 *       and finding the max they all cover together etc.
 *
 * @author Alex Levenson
 */
class SparkPlatform(
  sc: SparkContext,
  timeSpan: Interval[Timestamp]
) extends Platform[SparkPlatform] with PlatformPlanner[SparkPlatform] {

  override type Source[T] = SparkSource[T]
  override type Store[K, V] = SparkStore[K, V]
  override type Sink[T] = SparkSink[T]
  override type Service[K, LV] = SparkService[K, LV]
  override type Plan[T] = RDD[(Timestamp, T)]

  // TODO: remove statefulness from this platform
  private[this] val writeClosures: ArrayBuffer[() => Unit] = ArrayBuffer()
  private[this] var planned = false

  override def plan[T](completed: TailProducer[SparkPlatform, T]): SparkPlatform#Plan[T] = {
    assert(!planned, "This platform has already been planned")
    planned = true
    visit(completed).plan
  }

  override def planNamedProducer[T](prod: Prod[T], visited: Visited): PlanState[T] = {
    toPlan(prod, visited)
  }

  override def planIdentityKeyedProducer[K, V](prod: Prod[(K, V)], visited: Visited): PlanState[(K, V)] = {
    toPlan(prod, visited)
  }

  override def planSource[T](source: Source[T], visited: Visited): PlanState[T] = {
    PlanState(source.rdd(sc, timeSpan), visited)
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
    @transient fn: (T) => TraversableOnce[U]): PlanState[U] = {

    val extFn = Externalizer(fn)

    val planState = toPlan(prod, visited)
    val flatMapped = planState.plan.flatMap { case (ts, v) =>
      extFn.get(v).map { u => (ts, u) }
    }

    PlanState(flatMapped, planState.visited)
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
    @transient fn: K => TraversableOnce[K2]): PlanState[(K2, V)] = {

    val extFn = Externalizer(fn)

    val planState = toPlan(prod, visited)
    val mapped = planState.plan.flatMap {
      case (ts, (key, value)) => extFn.get(key).map { k2 => (ts, (k2, value)) }
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
      sink.write(sc, planState.plan, timeSpan)
    }
    planState
  }

  override def planLeftJoinedProducer[K: ClassTag, V: ClassTag, JoinedV](prod: Prod[(K, V)], visited: Visited, service: Service[K, JoinedV]):
    PlanState[(K, (V, Option[JoinedV]))] = {

    val planState = toPlan(prod, visited)

    val joined = service.lookup(sc, timeSpan, planState.plan)

    PlanState(joined, planState.visited)
  }

  override def planSummer[K: ClassTag, V: ClassTag](
    prod: Prod[(K, V)],
    visited: Visited,
    store: Store[K, V],
    monoid: Monoid[V]): PlanState[(K, (Option[V], V))] = {

    val planState = toPlan(prod, visited)
    // TODO: detect commutativity
    val mergeResult = store.merge(sc, timeSpan, planState.plan, NonCommutative, monoid)

    writeClosures += mergeResult.writeClosure

    PlanState(mergeResult.sumBeforeMerge, planState.visited)
  }

  def run(): Unit = {
    assert(planned, "This platform was not planned")
    // TODO: thread pool
    writeClosures.foreach { c => c.apply() }
  }

  def run[T](completed: TailProducer[SparkPlatform, T]): Unit = {
    plan(completed)
    run()
  }

}