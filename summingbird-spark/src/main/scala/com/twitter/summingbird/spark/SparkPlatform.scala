package com.twitter.summingbird.spark

import com.twitter.algebird.{ Semigroup, Interval }
import com.twitter.summingbird._
import com.twitter.summingbird.batch.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ ArrayBuffer, Buffer => ScalaBuffer }
import scala.reflect.ClassTag
import com.twitter.summingbird.option.{ MonoidIsCommutative, Commutativity, NonCommutative, Commutative }
import com.twitter.chill.Externalizer
import scala.collection.mutable
import scalaz.Reader

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
    timeSpan: Interval[Timestamp],
    options: Map[String, Options] = Map.empty) extends Platform[SparkPlatform] with PlatformPlanner[SparkPlatform] {

  override type Source[T] = SparkSource[T]
  override type Store[K, V] = SparkStore[K, V]
  override type Sink[T] = SparkSink[T]
  override type Service[K, LV] = SparkService[K, LV]
  override type Plan[T] = RDD[(Timestamp, T)]

  // TODO: remove statefulness from this platform
  private[this] val writeClosures: ScalaBuffer[() => Unit] = ArrayBuffer()
  private[this] var planned = false
  private[this] val namedProducers = mutable.Map[Prod[_], String]()

  override def plan[T](completed: TailProducer[SparkPlatform, T]): SparkPlatform#Plan[T] = {
    assert(!planned, "This platform has already been planned")
    planned = true
    visit(completed).plan
  }

  override def planNamedProducer[T](prod: Prod[T], name: String, visited: Visited): PlanState[T] = {
    namedProducers.put(prod, name)
    toPlan(prod, visited)
  }

  override def planIdentityKeyedProducer[K, V](prod: Prod[(K, V)], visited: Visited): PlanState[(K, V)] = {
    toPlan(prod, visited)
  }

  override def planSource[T](source: Source[T], visited: Visited): PlanState[T] = {
    PlanState(source.rdd(timeSpan)(sc), visited)
  }

  override def planOptionMappedProducer[T, U: ClassTag](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => Option[U]): PlanState[U] = {

    planFlatMappedProducer[T, U](prod, visited, x => fn(x))
  }

  override def planFlatMappedProducer[T, U: ClassTag](
    prod: Prod[T],
    visited: Visited,
    @transient fn: (T) => TraversableOnce[U]): PlanState[U] = {

    val extFn = Externalizer(fn)

    val planState = toPlan(prod, visited)
    val flatMapped = planState.plan.flatMap {
      case (ts, v) =>
        extFn.get(v).map { u => (ts, u) }
    }

    planState.copy(plan = flatMapped)
  }

  override def planValueFlatMappedProducer[K, V, U: ClassTag](
    prod: Prod[(K, V)],
    visited: Visited,
    @transient fn: (V) => TraversableOnce[U]): PlanState[(K, U)] = {

    val extFn = Externalizer(fn)

    val planState = toPlan(prod, visited)
    val valueFlatMapped = planState.plan.flatMap {
      case (ts, (k, v)) =>
        extFn.get(v).map { u => (ts, (k, u)) }
    }

    planState.copy(plan = valueFlatMapped)
  }

  override def planMergedProducer[T](left: Prod[T], right: Prod[T], visited: Visited): PlanState[T] = {
    // plan down both sides of the tree
    val leftPlanState = toPlan(left, visited)
    val rightPlanState = toPlan(right, leftPlanState.visited)

    // when those are both done, union the results
    val both = leftPlanState.plan ++ rightPlanState.plan
    rightPlanState.copy(plan = both)
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

    planState.copy(plan = mapped)
  }

  override def planAlsoProducer[E, R: ClassTag](ensure: TailProd[E], result: Prod[R], visited: Visited): PlanState[R] = {
    val ensurePlanState = toPlan(ensure, visited)
    val resultPlanState = toPlan(result, ensurePlanState.visited)
    resultPlanState
  }

  override def planWrittenProducer[T: ClassTag](prod: Prod[T], visited: Visited, sink: Sink[T]): PlanState[T] = {
    val planState = toPlan(prod, visited)
    writeClosures += { () => sink.write(planState.plan, timeSpan)(sc) }
    planState
  }

  override def planLeftJoinedProducer[K: ClassTag, V: ClassTag, JoinedV](prod: Prod[(K, V)], visited: Visited, service: Service[K, JoinedV]): PlanState[(K, (V, Option[JoinedV]))] = {

    val planState = toPlan(prod, visited)

    val joined = service.lookup(timeSpan, planState.plan)(sc)

    planState.copy(plan = joined)
  }

  override def planSummer[K: ClassTag, V: ClassTag](
    summer: Summer[SparkPlatform, K, V],
    prod: Prod[(K, V)],
    visited: Visited,
    store: Store[K, V],
    semigroup: Semigroup[V]): PlanState[(K, (Option[V], V))] = {

    val planState = toPlan(prod, visited)

    val commutativity = getCommutativity(summer)

    val mergeResult = store.merge(timeSpan, planState.plan, commutativity, semigroup)(sc)

    writeClosures += mergeResult.writeClosure

    planState.copy(plan = mergeResult.sumBeforeMerge)
  }

  def getCommutativity(summer: Summer[SparkPlatform, _, _]): Commutativity = {
    val name = namedProducers.get(summer).getOrElse("DEFAULT")
    val commutativity = for {
      opt <- options.get(name)
      comm <- opt.get[MonoidIsCommutative]
    } yield {
      comm.commutativity
    }
    commutativity.getOrElse(MonoidIsCommutative.default.commutativity)
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
