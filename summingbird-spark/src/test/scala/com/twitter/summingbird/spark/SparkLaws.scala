package com.twitter.summingbird.spark

import com.twitter.algebird.{Monoid, Empty, Interval}
import com.twitter.summingbird.batch.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.specs2.mutable.Specification
import com.twitter.summingbird.{Producer, Source, TestGraphs}
import org.scalacheck.Arbitrary

// TODO: support time
class SparkLaws extends Specification {

  def memSource[T](s: Seq[T]) = new SparkSource[T] {
    override def rdd(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(Timestamp, T)] = {
      sc.makeRDD(s.map {
        x => (Timestamp(1), x)
      })
    }
  }

  def memStore[K, V](m: Map[K, V]) = new SimpleSparkStore[K, V] {
    var result: Seq[(Timestamp, (K, (Option[V], V)))] = _

    override def snapshot(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(K, V)] = sc.makeRDD(m.toSeq)

    override def write(sc: SparkContext, updatedSnapshot: RDD[(Timestamp, (K, (Option[V], V)))]): Unit = {
      result = updatedSnapshot.toArray()
    }
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  "SparkPlatform" should {

    "match scala for single step jobs" in {
      // TODO: reconcile duplication w/ ScaldingLaws
      /*
      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]
      */

      val original = List(101, 200, 300)
      val fn: Int => List[(Int, Int)] = x => List((x%2, x))
      val initStore = Map(1000 -> 12)

      val inMemory = TestGraphs.singleStepInScala(original)(fn)

      val sc = new SparkContext("local", "TestJob")

      val source = memSource(original)
      val store = memStore(initStore)

      val job = TestGraphs.singleStepJob[SparkPlatform, Int, Int, Int](Source[SparkPlatform, Int](source), store)(fn)

      val platform = new SparkPlatform(sc, Empty())
      val plan = platform.plan(job)
      platform.run()
      println(store.result)
      println(inMemory)
      val sparkResult = store.result.map { case (ts, (k, (oldV, v))) => (k, v) }.toMap

      assert(sparkResult === Monoid.plus(initStore, inMemory))
    }

  }

}