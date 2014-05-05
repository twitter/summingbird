package com.twitter.summingbird.spark

import com.twitter.algebird.{Group, Monoid, Empty, Interval}
import com.twitter.summingbird.batch.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.specs2.mutable.Specification
import com.twitter.summingbird.{Producer, Source, TestGraphs}
import org.scalacheck.{Gen, Arbitrary}
import scala.reflect.ClassTag

// TODO: support time
// TODO: reconcile duplication w/ ScaldingLaws
class SparkLaws extends Specification {

  def memSource[T](s: Seq[T]) = new SparkSource[T] {
    override def rdd(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(Timestamp, T)] = {
      sc.makeRDD(s.map {
        x => (Timestamp(1), x)
      })
    }
  }

  def memStore[K : ClassTag, V : ClassTag](m: Map[K, V]) = new SimpleSparkStore[K, V] {
    var result: Map[K, V] = _

    override def snapshot(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(K, V)] = sc.makeRDD(m.toSeq)

    override def write(sc: SparkContext, updatedSnapshot: RDD[(K, V)]): Unit = {
      result = updatedSnapshot.toArray().toMap
    }
  }

  def assertSparseEqual[K, V : Group](found: Map[K, V], expected: Map[K, V], name: String = "found map"): Unit = {
    val diff = Group.minus(expected, found)
    val wrong = Monoid.isNonZero(diff)
    if (wrong) {
      val foundFiltered = found.filter { case (k, v) => Monoid.isNonZero(v) }
      val expectedFiltered = expected.filter { case (k, v) => Monoid.isNonZero(v) }
      val extra = foundFiltered -- expectedFiltered.keySet
      val missing = expectedFiltered -- foundFiltered.keySet
      val wrongEntries = expectedFiltered
        .filterNot { case (k, v) => found.get(k) == Some(v) }
        .map { case (k, v) => "[found: %s, expected: %s]".format((k, found(k)), (k,v)) }

      val msg = Seq(
        "%s is wrong".format(name),
        "Extra entries: " + extra,
        "Missing entries: " + missing,
        "Wrong entries: " + wrongEntries
      ).mkString("\n")
      assert(false, msg)
    }
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  "SparkPlatform" should {

    "match scala for single step jobs" in {


      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]

      val inMemory = TestGraphs.singleStepInScala(original)(fn)

      val sc = new SparkContext("local", "TestJob")

      val source = memSource(original)
      val store = memStore(initStore)

      val job = TestGraphs.singleStepJob[SparkPlatform, Int, Int, Int](Source[SparkPlatform, Int](source), store)(fn)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResult = store.result
      val memResult = Monoid.plus(initStore, inMemory)
      assertSparseEqual(sparkResult, memResult)
    }

  }

}