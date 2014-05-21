package com.twitter.summingbird.spark

import com.twitter.algebird.Empty
import com.twitter.algebird._
import com.twitter.summingbird.Source
import com.twitter.summingbird._
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.option.{Commutative, NonCommutative, Commutativity, MonoidIsCommutative}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalacheck.Arbitrary
import org.specs2.mutable.Specification
import scala.reflect.ClassTag
import scala.util.Random

// TODO: consider whether using mockito / easymock mocks here makes more sense
object SparkLaws {

  def untimedMemSource[T](s: Seq[T]) = timedMemSource(s.map{ x => (Timestamp(1), x) })
  
  def timedMemSource[T](s: Seq[(Timestamp, T)]) = new SparkSource[T] {
    override def rdd(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(Timestamp, T)] = {
      sc.makeRDD(s)
    }
  }

  def memStore[K : ClassTag, V : ClassTag](m: Map[K, V]) = new SimpleSparkStore[K, V] {
    var result: Map[K, V] = _
    var commutativity: Commutativity = _

    override def merge(sc: SparkContext,
                       timeSpan: Interval[Timestamp],
                       deltas: RDD[(Timestamp, (K, V))],
                       commutativity: Commutativity,
                       semigroup: Semigroup[V]): MergeResult[K, V] = {

      this.commutativity = commutativity
      super.merge(sc, timeSpan, deltas, commutativity, semigroup)
    }

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

  def memService[K, LV](m: Map[K, LV]): SparkService[K, LV] = new SparkService[K, LV] {
    override def lookup[V](sc: SparkContext, timeSpan: Interval[Timestamp], rdd: RDD[(Timestamp, (K, V))]): RDD[(Timestamp, (K, (V, Option[LV])))] = {
      rdd.map { case (ts, (k, v)) => (ts, (k, (v, m.get(k)))) }
    }
  }

  def memService[K, LV](f: K => Option[LV], keys: Set[K]): SparkService[K, LV] = {
    val m = keys.flatMap {
      k => f(k).map {
        v => (k, v) }
    }.toMap

    memService(m)
  }

  case class SparseDiff[K, V](
    extra: Map[K, V],
    missing: Map[K, V],
    wrong: Seq[String]
  ) {
    val explainDiff: String = Seq(
      "Extra entries: " + extra,
      "Missing entries: " + missing,
      "Wrong entries: " + wrong
    ).mkString("\n")

    val isEmpty = extra.isEmpty && missing.isEmpty && wrong.isEmpty
  }

  def sparseDiff[K, V : Monoid](found: Map[K, V], expected: Map[K, V]): SparseDiff[K, V] = {
    val foundFiltered = found.filter { case (k, v) => Monoid.isNonZero(v) }
    val expectedFiltered = expected.filter { case (k, v) => Monoid.isNonZero(v) }
    val extra = foundFiltered -- expectedFiltered.keySet
    val missing = expectedFiltered -- foundFiltered.keySet
    val wrongEntries = expectedFiltered
      .filter { case (k, v) => found.contains(k) && found(k) != v }
      .map { case (k, v) => "[found: %s, expected: %s]".format((k, found(k)), (k,v)) }

    SparseDiff(extra, missing, wrongEntries.toSeq)
  }

  def assertNotSparseEqual[K, V : Monoid](found: Map[K, V], expected: Map[K, V], name: String = "found map"): Unit = {
    val diff = sparseDiff(found, expected)
    assert(!diff.isEmpty, "%s should not be equal to expected".format(name))
  }

  def assertSparseEqual[K, V : Monoid](found: Map[K, V], expected: Map[K, V], name: String = "found map"): Unit = {
    val diff = sparseDiff(found, expected)
    assert(diff.isEmpty, "%s is wrong\n".format(name) + diff.explainDiff)
  }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get
}

// TODO: support time
//
// TODO: reconcile duplication w/ ScaldingLaws
// TODO: Maybe make a generic PlatformLaws base class / trait
class SparkLaws extends Specification {

  import SparkLaws._

  // TODO: Ideally this wouldn't be shared, but spark goes into an infinite crash loop
  // if you use more than one SparkContext from the same jvm for some reason :(
  val sc = new SparkContext("local", "LocalTestCluster")

  "SparkPlatform" should {

    "match scala for single step jobs" in {

      val original = sample[List[Int]]
      val fn = sample[(Int) => List[(Int, Int)]]
      val initStore = sample[Map[Int, Int]]

      val inMemory = TestGraphs.singleStepInScala(original)(fn)

      val source = untimedMemSource(original)
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

      val source = untimedMemSource(original)
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

      val source = untimedMemSource(original)
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

      val source = untimedMemSource(original)
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

      val inMemory = TestGraphs.leftJoinInScala(original)(serviceFn)(preJoinFn)(postJoinFn)

      val source = untimedMemSource(original)
      val store = memStore(initStore)

      val serviceKeys = original.flatMap(preJoinFn).map {_._1}.toSet
      val service = memService(serviceFn, serviceKeys)

      val job = TestGraphs.leftJoinJob[SparkPlatform, Int, Int, Int, Int, Int](Source[SparkPlatform, Int](source), service, store)(preJoinFn)(postJoinFn)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResult = store.result
      val memResult = Monoid.plus(initStore, inMemory)
      assertSparseEqual(sparkResult, memResult)
    }

    "match scala for jobs with multiple summers" in {
      val original = sample[List[Int]]
      val fnR = sample[(Int) => List[Int]]
      val fnA = sample[(Int) => List[(Int, Int)]]
      val fnB = sample[(Int) => List[(Int, Int)]]
      val initStoreA = sample[Map[Int, Int]]
      val initStoreB = sample[Map[Int, Int]]

      val inMemory = TestGraphs.multipleSummerJobInScala(original)(fnR, fnA, fnB)

      val source = untimedMemSource(original)
      val storeA = memStore(initStoreA)
      val storeB = memStore(initStoreB)

      val job = TestGraphs.multipleSummerJob[SparkPlatform, Int, Int, Int, Int, Int, Int](Source[SparkPlatform, Int](source), storeA, storeB)(fnR, fnA, fnB)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResultA = storeA.result
      val sparkResultB = storeB.result
      val memResultA = Monoid.plus(initStoreA, inMemory._1)
      val memResultB = Monoid.plus(initStoreB, inMemory._2)
      assertSparseEqual(sparkResultA, memResultA)
      assertSparseEqual(sparkResultB, memResultB)
    }

    // TODO: is this different from above? Do we need both? Or just a better name?
    "match scala for jobs with two sumByKeys " in {
      val original = sample[List[(Int, Int)]]
      val fn = sample[(Int) => List[Int]]

      val initStoreA = sample[Map[Int, Int]]
      val initStoreB = sample[Map[Int, Int]]

      val inMemory = TestGraphs.twoSumByKeyInScala(original, fn)

      val source = untimedMemSource(original)
      val storeA = memStore(initStoreA)
      val storeB = memStore(initStoreB)

      val job = TestGraphs.twoSumByKey[SparkPlatform, Int, Int, Int](Source[SparkPlatform, (Int, Int)](source), storeA, fn, storeB)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      val sparkResultA = storeA.result
      val sparkResultB = storeB.result
      val memResultA = Monoid.plus(initStoreA, inMemory._1)
      val memResultB = Monoid.plus(initStoreB, inMemory._2)
      assertSparseEqual(sparkResultA, memResultA)
      assertSparseEqual(sparkResultB, memResultB)
    }

    "match scala for map only jobs" in {
      val original = sample[List[Int]]
      val fn = sample[(Int) => List[Int]]

      val inMemory = original.flatMap(fn)

      val source = untimedMemSource(original)
      val sink = captureSink[Int]

      val job = TestGraphs.mapOnlyJob[SparkPlatform, Int, Int](Source[SparkPlatform, Int](source), sink)(fn)

      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)
      assert(sink.result.map(_._2) === inMemory)
    }

    "match scala for a job with a lookup" in {
      val original = sample[List[Int]]
      val serviceFn = sample[Int => Option[Int]]

      val inMemory = TestGraphs.lookupJobInScala(original, serviceFn)

      val source = untimedMemSource(original)
      val service = memService(serviceFn, original.toSet)
      val sink = captureSink[(Int, Int)]

      val job = TestGraphs.lookupJob[SparkPlatform, Int, Int](Source[SparkPlatform, Int](source), service, sink)
      val platform = new SparkPlatform(sc, Empty())
      platform.run(job)

      assert(sink.result.map(_._2) === inMemory)

    }

    "assume semigroups are not commutative unless explicitly specified" in {
      commutativityTest(Map(), NonCommutative)
    }

    "respects global default commutativity" in {
      commutativityTest(Map("DEFAULT" -> Options().set(MonoidIsCommutative(false))), NonCommutative)
      commutativityTest(Map("DEFAULT" -> Options().set(MonoidIsCommutative(true))), Commutative, assertFail=true)
    }

    "respects explicitly specified commutativity" in {
      commutativityTest(Map("mySummer" -> Options().set(MonoidIsCommutative(false))), NonCommutative, summerName = Some("mySummer"))
      commutativityTest(Map("mySummer" -> Options().set(MonoidIsCommutative(true))), Commutative, summerName = Some("mySummer"), assertFail=true)
      commutativityTest(Map("unrelatedSummer" -> Options().set(MonoidIsCommutative(true))), NonCommutative, summerName = Some("mySummer"))
      commutativityTest(Map("unrelatedSummer" -> Options().set(MonoidIsCommutative(false))), NonCommutative, summerName = Some("mySummer"))
    }
  }

  private def commutativityTest(
    options: Map[String, Options],
    expectedCommutativity: Commutativity,
    summerName: Option[String] = None,
    assertFail: Boolean = false): Unit = {

    // make random k,v pairs
    val values = sample[List[(Int, Int)]].map { case (k, v) => (k, List(v)) }

    // pair them with time
    val time = (1 to values.size).map(Timestamp(_))

    // shuffle them
    val shuffled = Random.shuffle(time.zip(values))

    // ensure there are entries with the same key and out of order
    val original = shuffled ++ Seq(
      (Timestamp(6000), (7777, List(111112))),
      (Timestamp(3000), (7777, List(111113))),
      (Timestamp(8000), (7777, List(111114)))
    )

    val initStore = sample[Map[Int, List[Int]]]

    val sorted = original.sortBy(_._1).map(_._2)
    val inMemory = MapAlgebra.sumByKey[Int, List[Int]](sorted)

    val source = timedMemSource(original)
    val store = memStore(initStore)

    val summer = Source[SparkPlatform, (Int, List[Int])](source).sumByKey(store)
    val job = summerName.map { name => summer.name(name) }.getOrElse(summer)

    val platform = new SparkPlatform(sc, Empty(), options)
    platform.run(job)
    val sparkResult = store.result
    val memResult = Monoid.plus(initStore, inMemory)

    assert(store.commutativity === expectedCommutativity)

    if (!assertFail) {
      assertSparseEqual(sparkResult, memResult)
    } else {
      assertNotSparseEqual(sparkResult, memResult)
    }

  }

}