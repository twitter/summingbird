package com.twitter.summingbird.storm

import java.util.UUID

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.memory.Memory
import com.twitter.summingbird.online.MergeableStoreFactory
import com.twitter.summingbird.storm.Storm.toStormSource
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.summingbird._
import com.twitter.util.Future
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
    val memorySinkCreator = new MemorySinkCreator()
    val memoryCtx = new CreatorCtx(memorySourceCreator, memoryStoreCreator, memorySinkCreator)

    val stormSourceCreator = new StormSourceCreator(seed, params)
    val stormStoreCreator = new StormStoreCreator(seed, params)
    val stormSinkCreator = new StormSinkCreator()
    val stormCtx = new CreatorCtx(stormSourceCreator, stormStoreCreator, stormSinkCreator)

    val memory = new Memory()
    memory.run(memory.plan(producerCreator.apply(memoryCtx)))

//    assert(OnlinePlan(tail).nodes.size < 10)
    StormTestRun(producerCreator.apply(stormCtx))

    assertEquiv(memorySourceCreator.sources, stormSourceCreator.sources)
    assertEquiv(memoryStoreCreator.ids(), stormStoreCreator.ids())
    memoryStoreCreator.ids().foreach(id =>
      assertEquiv(memoryStoreCreator.get(id), stormStoreCreator.get(id))
    )
    assertEquiv(memorySinkCreator.get(), stormSinkCreator.get())
    stormSinkCreator.clear()
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
  def apply[P <: Platform[P]](ctx: CreatorCtx[P]): TailProducer[P, Any]
}

class CreatorCtx[P <: Platform[P]](
  sourceCreator: SourceCreator[P],
  storeCreator: StoreCreator[P],
  sinkCreator: SinkCreator[P]
) {
  def source[T: Arbitrary](id: String): Source[P, T] = sourceCreator(id)
  def store[K: Arbitrary, V: Arbitrary: Semigroup](id: String): P#Store[K, V] = storeCreator(id)
  def sink[V: Ordering](id: String): P#Sink[V] = sinkCreator(id)
}

trait SourceCreator[P <: Platform[P]] {
  def apply[T: Arbitrary](id: String): Source[P, T]
}

trait StoreCreator[P <: Platform[P]] {
  def apply[K: Arbitrary, V: Arbitrary: Semigroup](id: String): P#Store[K, V]
}

trait SinkCreator[P <: Platform[P]] {
  def apply[V: Ordering](id: String): P#Sink[V]
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
  override def apply[T: Arbitrary](id: String): Source[Memory, T] = Source[Memory, T](get(id))
}

class StormSourceCreator(seed: Seed, params: Gen.Parameters) extends BaseSourceCreator[Storm](seed, params) {
  override def apply[T: Arbitrary](id: String): Source[Storm, T] = {
    implicit def extractor[E]: TimeExtractor[E] = TimeExtractor(_ => 0L)
    Source[Storm, T](toStormSource(TraversableSpout(get(id))))
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

class SinkContent[V: Ordering]() {
  val buffer: mutable.ListBuffer[V] = mutable.ListBuffer[V]()

  def add(value: V): Unit = buffer.append(value)
  def toList: List[V] = buffer.toList.sorted(implicitly[Ordering[V]])
}

class MemorySinkCreator extends SinkCreator[Memory] {
  val sinks = mutable.Map[String, SinkContent[_]]()

  override def apply[V: Ordering](id: String): (V) => Unit = {
    val sink = sinks.getOrElseUpdate(id, { new SinkContent() }).asInstanceOf[SinkContent[V]]
    v => sink.add(v)
  }

  def get(): Map[String, List[_]] = sinks.mapValues(_.toList).toMap
}

object StormSinkCreator {
  val sinks = mutable.Map[UUID, SinkContent[_]]()
}

class StormSinkCreator() extends SinkCreator[Storm] {
  val uuids = mutable.Map[String, UUID]()

  override def apply[V: Ordering](id: String): StormSink[V] = {
    val uuid = uuids.getOrElseUpdate(id, UUID.randomUUID())
    new SinkFn[V](v => {
      val sink = StormSinkCreator.sinks.getOrElseUpdate(uuid, { new SinkContent() }).asInstanceOf[SinkContent[V]]
      sink.add(v)
      println("!"+v)
      Future.Unit
    })
  }

  def get(): Map[String, List[_]] = uuids.mapValues(StormSinkCreator.sinks.get(_).get.toList).toMap

  def clear(): Unit = uuids.values.foreach(uuid => StormSinkCreator.sinks.remove(uuid))
}
