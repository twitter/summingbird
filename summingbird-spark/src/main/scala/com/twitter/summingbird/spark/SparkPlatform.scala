package com.twitter.summingbird.spark

import com.twitter.summingbird._
import org.apache.spark.rdd.RDD
import com.twitter.summingbird.Source
import com.twitter.algebird.Monoid
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

trait SparkSink[T] {
  def write(t: T): Unit
}
// QUESTIONS:
// 1) Batching?
// 2) See Summer for more questions
class SparkPlatform extends Platform[SparkPlatform] with PlatformPlanner[SparkPlatform] {

  override type Source[T] = RDD[T]
  override type Store[K, V] = RDD[(K, V)]
  override type Sink[T] = SparkSink[T]
  override type Service[-K, +V] = (K => Option[V])
  override type Plan[T] = RDD[T]

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

  override def planOptionMappedProducer[T, U: ClassManifest](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => Option[U]): PlanState[U]  = {

    val planState = toPlan(prod, visited)
    PlanState(planState.plan.flatMap(fn(_)), planState.visited)
  }

  override def planFlatMappedProducer[T, U: ClassManifest](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => TraversableOnce[U]): PlanState[U] = {

    val planState = toPlan(prod, visited)
    PlanState(planState.plan.flatMap(fn(_)), planState.visited)
  }

  override def planMergedProducer[T](l: Prod[T], r: Prod[T], visited: Visited): PlanState[T] = {
    val leftPlanState = toPlan(l, visited)
    val rightPlanState = toPlan(r, leftPlanState.visited)
    PlanState(leftPlanState.plan ++ rightPlanState.plan, rightPlanState.visited)
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

  override def planAlsoProducer[E, R: ClassManifest](ensure: TailProd[E], result: Prod[R], visited: Visited): PlanState[R] = {
    val ensurePlanState = toPlan(ensure, visited)
    val resultPlanState = toPlan(result, ensurePlanState.visited)

    // TODO: don't do this :)
    // I'm thinking of eagerly calling spark actions, but in a thread pool and returning a Future
    // in that case type Plan would be a Future[RDD]
    val l: RDD[Either[E, R]] = ensurePlanState.plan.filter( _ => false ).map { Left(_) }
    val r: RDD[Either[E, R]] = resultPlanState.plan.map { Right(_) }
    val union = l ++ r
    val rOnly: RDD[R] = union.collect { case Right(x) => x }

    PlanState(rOnly, resultPlanState.visited)
  }

  override def planWrittenProducer[T: ClassManifest](prod: Prod[T], visited: Visited, sink: Sink[T]): PlanState[T]  = {
    val planState = toPlan(prod, visited)
    // TODO: Futures??
    val mapped = planState.plan.map { x => sink.write(x); x }
    PlanState(mapped, planState.visited)
  }

  override def planLeftJoinedProducer[K, V, JoinedV](prod: Prod[(K, V)], visited: Visited, service: Service[K, JoinedV]):
    PlanState[(K, (V, Option[JoinedV]))] = {

    val planState = toPlan(prod, visited)

    val joined = planState.plan.map {
      case (key, value) => (key, (value, service(key)))
    }

    PlanState(joined, planState.visited)
  }

  override def planSummer[K: ClassManifest, V: ClassManifest](
    prod: Prod[(K, V)],
    visited: Visited,
    store: Store[K, V],
    monoid: Monoid[V]): PlanState[(K, (Option[V], V))] = {

    val planState = toPlan(prod, visited)

    // QUESTION: can prod have duplicate keys here? I think so right?
    //           can store have duplicate keys here? I think not right?
    //           groupByKey can't be right -- that's going to put the whole group in memory!
    val summed = planState.plan.groupByKey().leftOuterJoin(store).map {
      case (key, (deltas, stored)) => {
        val deltaSum = deltas.foldLeft(monoid.zero) { (x, y) => monoid.plus(x, y) }
        val sum = stored.map { s =>  monoid.plus(s, deltaSum) }.getOrElse(deltaSum)
        (key, (stored, sum))
      }
    }
    PlanState(summed, planState.visited)
  }

}

object Go extends App {
  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Simple App")
    val sentencesRDD = sc.makeRDD(Seq("hello I am alex", "alex I am", "Who am I"))
    val src = Source[SparkPlatform, String](sentencesRDD)
    val store = sc.makeRDD(Seq("hello" -> 1000L, "who" -> 3000L))

    val job = makeJob[SparkPlatform](src, store)
    println(job)

    val plat = new SparkPlatform
    val plan = plat.plan(job)
    println(plan.toDebugString)
    val results = plan.toArray().toSeq
    println(results)
  }

  def makeJob[P <: Platform[P]](source: Producer[P, String], store: P#Store[String, Long]) = {
    source
      .flatMap { _.toLowerCase.split(" ") }
      .map { _ -> 1L }
      .sumByKey(store)
  }
}