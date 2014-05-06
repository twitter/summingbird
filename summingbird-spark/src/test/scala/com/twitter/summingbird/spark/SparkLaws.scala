package com.twitter.summingbird.spark

import com.twitter.algebird.{Group, Monoid, Empty, Interval}
import com.twitter.summingbird.batch.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.specs2.mutable.Specification
import com.twitter.summingbird.{Producer, Source, TestGraphs}
import org.scalacheck.{Gen, Arbitrary}
import scala.reflect.ClassTag

object SparkLaws {

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

  def captureSink[T] = new SparkSink[T] {
    var result: Seq[(Timestamp, T)] = _

    override def write(sc: SparkContext, rdd: RDD[(Timestamp, T)], timeSpan: Interval[Timestamp]): Unit = {
      result = rdd.toArray().toSeq
    }
  }

  def memService[K, LV](m: Map[K, LV]) = new SparkService[K, LV] {
    override def lookup[V](sc: SparkContext, timeSpan: Interval[Timestamp], rdd: RDD[(Timestamp, (K, V))]): RDD[(Timestamp, (K, (V, Option[LV])))] = {
      rdd.map { case (ts, (k, v)) => (ts, (k, (v, m.get(k)))) }
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
        .filter { case (k, v) => found.contains(k) && found(k) != v }
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

}

// TODO: support time
//
// TODO: reconcile duplication w/ ScaldingLaws
// TODO: Maybe make a generic PlatformLaws base class / trait
class SparkLaws extends Specification {
  import SparkLaws._

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  // TODO: Ideally this wouldn't be shared, but spark goes into an infinite crash loop
  // if you use more than one SparkContext from the same jvm for some reason :(
  val sc = new SparkContext("local", "LocalTestCluster")

  "SparkPlatform" should {

    "match scala for single step jobs" in {

      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]

      val inMemory = TestGraphs.singleStepInScala(original)(fn)

      val source = memSource(original)
      val store = memStore(initStore)

      val job = TestGraphs.singleStepJob[SparkPlatform, Int, Int, Int](Source[SparkPlatform, Int](source), store)(fn)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResult = store.result
      val memResult = Monoid.plus(initStore, inMemory)
      assertSparseEqual(sparkResult, memResult)
    }

    "match scala for jobs with a diamond" in {
      val original = sample[List[Int]]
      val fnA = sample[(Int) => List[(Int, Int)]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]

      val inMemory = TestGraphs.diamondJobInScala(original)(fnA)(fnB)

      val source = memSource(original)
      val store = memStore(initStore)
      val sink = captureSink[Int]

      val job = TestGraphs.diamondJob[SparkPlatform, Int, Int, Int](Source[SparkPlatform, Int](source), sink, store)(fnA)(fnB)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResult = store.result
      val memResult = Monoid.plus(initStore, inMemory)
      assertSparseEqual(sparkResult, memResult)
      assert(original === sink.result.map(_._2))
    }

    // TODO: better description
    "match scala for twinStepOptionMapFlatMap" in {
      val original = sample[List[Int]]
      val fnA = sample[(Int) => Option[Int]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]

      val inMemory = TestGraphs.twinStepOptionMapFlatMapScala(original)(fnA, fnB)

      val source = memSource(original)
      val store = memStore(initStore)

      val job = TestGraphs.twinStepOptionMapFlatMapJob[SparkPlatform, Int, Int, Int, Int](Source[SparkPlatform, Int](source), store)(fnA, fnB)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResult = store.result
      val memResult = Monoid.plus(initStore, inMemory)
      assertSparseEqual(sparkResult, memResult)
    }

    "match scala for jobs with single step map keys" in {
      val original = sample[List[Int]]
      val fnA = sample[(Int) => List[(Int, Int)]]
      val fnB = sample[(Int) => List[(Int)]]
      val initStore = sample[Map[Int, Int]]

      val inMemory = TestGraphs.singleStepMapKeysInScala(original)(fnA, fnB)

      val source = memSource(original)
      val store = memStore(initStore)

      val job = TestGraphs.singleStepMapKeysJob[SparkPlatform, Int, Int, Int, Int](Source[SparkPlatform, Int](source), store)(fnA, fnB)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResult = store.result
      val memResult = Monoid.plus(initStore, inMemory)
      assertSparseEqual(sparkResult, memResult)
    }

    "match scala for jobs with a left join" in {
      val original = sample[List[Int]]
      val preJoinFn = sample[(Int) => List[(Int, Int)]]
      val serviceFn = sample[Int => Option[Int]]
      val postJoinFn = sample[((Int, (Int, Option[Int]))) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]

      val serviceMap = original
        .flatMap(preJoinFn)
        .flatMap { case (k, v) =>
          serviceFn(k).map { lv => (k, lv) }
        }.toMap

      val inMemory = TestGraphs.leftJoinInScala(original)(serviceFn)(preJoinFn)(postJoinFn)

      val source = memSource(original)
      val store = memStore(initStore)
      val service = memService(serviceMap)

      val job = TestGraphs.leftJoinJob[SparkPlatform, Int, Int, Int, Int, Int](Source[SparkPlatform, Int](source), service, store)(preJoinFn)(postJoinFn)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResult = store.result
      val memResult = Monoid.plus(initStore, inMemory)
      assertSparseEqual(sparkResult, memResult)
    }

  }

}