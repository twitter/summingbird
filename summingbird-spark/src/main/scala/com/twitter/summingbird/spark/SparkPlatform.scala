package com.twitter.summingbird.spark

import com.twitter.summingbird._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import com.twitter.algebird.Monoid
import org.apache.spark.SparkContext._
import com.twitter.util.{ExecutorServiceFuturePool, Promise, FuturePool, Future}
import java.util.concurrent._
import com.twitter.concurrent.NamedPoolThreadFactory

trait SparkSink[T] {
  // go ahead and block in this function
  // it is being submit in a future pool
  def write(rdd: RDD[T]): Unit
}

// QUESTION: Where does batching come into this?
class SparkPlatform
    extends Platform[SparkPlatform] with PlatformPlanner[SparkPlatform] {

  override type Source[T] = RDD[T]
  override type Store[K, V] = RDD[(K, V)]
  override type Sink[T] = SparkSink[T]
  override type Service[K, V] = RDD[(K, V)]
  override type Plan[T] = Future[RDD[T]]

  private val pool = new WaitingFuturePool(
    FuturePool(Executors.newCachedThreadPool(new NamedPoolThreadFactory("SparkPlatformPool", true))))

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

  override def planOptionMappedProducer[T, U: ClassManifest](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => Option[U]): PlanState[U]  = {

    val planState = toPlan(prod, visited)
    PlanState(planState.plan.map {rdd => rdd.flatMap(fn(_)) }, planState.visited)
  }

  override def planFlatMappedProducer[T, U: ClassManifest](
    prod: Prod[T],
    visited: Visited,
    fn: (T) => TraversableOnce[U]): PlanState[U] = {

    val planState = toPlan(prod, visited)
    PlanState(planState.plan.map { rdd => rdd.flatMap(fn(_)) }, planState.visited)
  }

  override def planMergedProducer[T](l: Prod[T], r: Prod[T], visited: Visited): PlanState[T] = {
    val leftPlanState = toPlan(l, visited)
    val rightPlanState = toPlan(r, leftPlanState.visited)
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

  override def planAlsoProducer[E, R: ClassManifest](ensure: TailProd[E], result: Prod[R], visited: Visited): PlanState[R] = {
    val ensurePlanState = toPlan(ensure, visited)
    val resultPlanState = toPlan(result, ensurePlanState.visited)

    // QUESTION
    // better way to force execution?
    // does order of execution matter?
    val e = ensurePlanState.plan.flatMap {
      rdd => pool { rdd.foreach(x => Unit) }
    }

    val ret = Future.join(e, resultPlanState.plan).map { case (_, x) => x }

    PlanState(ret, resultPlanState.visited)
  }

  override def planWrittenProducer[T: ClassManifest](prod: Prod[T], visited: Visited, sink: Sink[T]): PlanState[T]  = {
    val planState = toPlan(prod, visited)

    val written = planState.plan.flatMap {
      rdd => pool { sink.write(rdd) }
    }

    // QUESTION
    // does order of execution matter?
    val ret = Future.join(written, planState.plan).map { case (_, x) => x }

    PlanState(ret, planState.visited)
  }

  override def planLeftJoinedProducer[K: ClassManifest, V: ClassManifest, JoinedV](prod: Prod[(K, V)], visited: Visited, service: Service[K, JoinedV]):
    PlanState[(K, (V, Option[JoinedV]))] = {

    val planState = toPlan(prod, visited)

    val joined = planState.plan.map { rdd =>
      val pair: PairRDDFunctions[K, V] = rdd
      pair.leftOuterJoin(service)
    }

    PlanState(joined, planState.visited)
  }

  override def planSummer[K: ClassManifest, V: ClassManifest](
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

class WaitingFuturePool(delegate: ExecutorServiceFuturePool) extends FuturePool {
  private val lock = new Object
  private val waitPromise = new Promise[Unit]
  private val completionPool: ExecutorServiceFuturePool =
      FuturePool(Executors.newSingleThreadExecutor(
        new NamedPoolThreadFactory("SparkPlatformCompletionPool", true)))

  private var count = 0
  private var latch: CountDownLatch = _
  private var started = false

  def apply[T](f: => T): Future[T] = {
    lock synchronized {
      count = count + 1
      waitPromise.flatMap {
        _ => delegate(f)
      } ensure {
        latch.countDown()
      }
    }
  }

  def start(): Future[Unit] = {
    lock synchronized {
      if (started) {
        throw new IllegalStateException("This WaitingFuturePool has already been started!")
      }

      started = true
      latch = new CountDownLatch(count)

      // start the future pool
      waitPromise.setValue(Unit)

      // create a Future for when the future pool has completed
      val shutdown: Future[Unit] = completionPool {
        latch.await()
        delegate.executor.shutdownNow()
        delegate.executor.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
      }

      shutdown ensure {
        completionPool.executor.shutdownNow()
      }

    }
  }

}