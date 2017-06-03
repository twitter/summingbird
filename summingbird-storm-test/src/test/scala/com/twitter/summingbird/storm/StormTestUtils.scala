package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.memory.Memory
import com.twitter.summingbird.online.MergeableStoreFactory
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.summingbird.{Platform, TailProducer, TimeExtractor}
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Cogen, Gen}

import scala.collection.mutable

object StormTestUtils {
  def testStormEqualToMemory(producerCreator: ProducerCreator)(implicit storm: Storm): Unit = {
    testStormEqualToMemory(producerCreator, Seed.random(), Gen.Parameters.default)
  }

  def testStormEqualToMemory(producerCreator: ProducerCreator, seed: Seed, params: Gen.Parameters)(implicit storm: Storm): Unit = {
    val memorySourceCreator = new MemorySourceCreator(seed, params)
    val memoryStoreCreator = new MemoryStoreCreator(seed, params)
    val stormSourceCreator = new StormSourceCreator(seed, params)
    val stormStoreCreator = new StormStoreCreator(seed, params)

    val memory = new Memory()
    memory.run(memory.plan(producerCreator.apply[Memory](memorySourceCreator, memoryStoreCreator)))

//    assert(OnlinePlan(tail).nodes.size < 10)
    StormTestRun(producerCreator.apply[Storm](stormSourceCreator, stormStoreCreator))

    assertEquiv(memorySourceCreator.sources, stormSourceCreator.sources)
    assertEquiv(memoryStoreCreator.ids(), stormStoreCreator.ids())
    memoryStoreCreator.ids().foreach(id =>
      assertEquiv(memoryStoreCreator.get(id), stormStoreCreator.get(id))
    )
  }

  def sample[T: Arbitrary](seed: Seed, params: Gen.Parameters, id: String): T = {
    implicitly[Arbitrary[T]].arbitrary(
      params,
      implicitly[Cogen[String]].perturb(seed, id)
    ).get
  }

  def assertEquiv[T](expected: T, returned: T)(implicit equiv: Equiv[T]): Unit = {
    assert(equiv.equiv(expected, returned), (expected.toString, returned.toString))
  }
}

trait ProducerCreator {
  def apply[P <: Platform[P]](createSource: SourceCreator[P], createStore: StoreCreator[P]): TailProducer[P, Any]
}

trait SourceCreator[P <: Platform[P]] {
  def apply[T: Arbitrary](id: String): P#Source[T]
}

trait StoreCreator[P <: Platform[P]] {
  def apply[K: Arbitrary, V: Arbitrary: Semigroup](id: String): P#Store[K, V]
}

abstract class BaseSourceCreator[P <: Platform[P]](seed: Seed, params: Gen.Parameters) extends SourceCreator[P] {
  val sources: mutable.Map[String, List[_]] = mutable.Map()

  def get[T: Arbitrary](id: String): List[T] =
    sources.getOrElseUpdate(id, {
      StormTestUtils.sample[List[T]](seed, params, id)
    }).asInstanceOf[List[T]]

  def ids(): Set[String] = sources.keys.toSet
}

class MemorySourceCreator(seed: Seed, params: Gen.Parameters) extends BaseSourceCreator[Memory](seed, params) {
  override def apply[T: Arbitrary](id: String): TraversableOnce[T] = get(id)
}

class StormSourceCreator(seed: Seed, params: Gen.Parameters) extends BaseSourceCreator[Storm](seed, params) {
  override def apply[T: Arbitrary](id: String): StormSource[T] = {
    implicit def extractor[E]: TimeExtractor[E] = TimeExtractor(_ => 0L)
    Storm.toStormSource(TraversableSpout(get(id)))
  }
}

class MemoryStoreCreator(seed: Seed, params: Gen.Parameters) extends StoreCreator[Memory] {
  val stores: mutable.Map[String, mutable.Map[_, _]] = mutable.Map()

  override def apply[K: Arbitrary, V: Arbitrary: Semigroup](id: String): mutable.Map[K, V] =
    stores.getOrElseUpdate(id, {
      val initial = StormTestUtils.sample[Map[K, V]](seed, params, id)
      mutable.Map[K, V](initial.toSeq: _*)
    }).asInstanceOf[mutable.Map[K, V]]

  def ids(): Set[String] = stores.keys.toSet
  def get(id: String): Map[_, _] = stores.get(id).get.toMap
}

class StormStoreCreator(seed: Seed, params: Gen.Parameters) extends StoreCreator[Storm] {
  val stores: mutable.Map[String, (String, MergeableStoreFactory[(_, BatchID), _])] = mutable.Map()

  override def apply[K: Arbitrary, V: Arbitrary : Semigroup](id: String): MergeableStoreFactory[(K, BatchID), V] =
    stores.getOrElseUpdate(id, {
      val initial = StormTestUtils.sample[Map[K, V]](seed, params, id)
      TestStore.createStore(initial).asInstanceOf[(String, MergeableStoreFactory[(_, BatchID), _])]
    })._2.asInstanceOf[MergeableStoreFactory[(K, BatchID), V]]

  def ids(): Set[String] = stores.keys.toSet
  def get(id: String): Map[_, _] = TestStore(stores.get(id).get._1).get.toScala
}
