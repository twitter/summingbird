package com.twitter.summingbird

import com.twitter.summingbird.memory.{ Memory, MemoryService }
import scala.collection.mutable

object MemoryTestExecutor {
  def apply[T](producer: TailProducer[TestPlatform, T]): TestResult = {
    val transformer = new TestToMemoryTranformer()
    val memoryProducer = PlatformTransformer(transformer, producer).asInstanceOf[TailProducer[Memory, T]]
    val memory = new Memory()
    memory.run(memory.plan(memoryProducer))
    TestResult(transformer.stores.mapValues(_.toMap).toMap, transformer.sinks.mapValues(_.toList).toMap)
  }

  case class TestResult(stores: Map[String, Map[_, _]], sinks: Map[String, List[_]])

  class SinkContent[V]() {
    val buffer: mutable.ListBuffer[V] = mutable.ListBuffer[V]()

    def add(value: V): Unit = buffer.append(value)
    def toList: List[V] = buffer.toList
  }
}

private class TestToMemoryTranformer extends PlatformTransformer[TestPlatform, Memory] {
  // This fields shouldn't be threadsafe because our `Memory` platform is single threaded.
  val stores: mutable.Map[String, mutable.Map[_, _]] = mutable.Map()
  val sinks: mutable.Map[String, MemoryTestExecutor.SinkContent[_]] = mutable.Map()

  override def transformSource[T](source: TestPlatform.SourceList[T]): TraversableOnce[T] =
    source.data

  override def transformSink[T](source: TestPlatform.Sink[T]): (T) => Unit = {
    val sink = sinks.getOrElseUpdate(source.id, {
      new MemoryTestExecutor.SinkContent[T]()
    }).asInstanceOf[MemoryTestExecutor.SinkContent[T]]
    v => sink.add(v)
  }

  override def transformStore[K, V](source: TestPlatform.Store[K, V]): mutable.Map[K, V] =
    stores.getOrElseUpdate(source.id, {
      mutable.Map[K, V](source.initial.toSeq: _*)
    }).asInstanceOf[mutable.Map[K, V]]

  override def transformService[K, V](source: TestPlatform.Service[K, V]): MemoryService[K, V] =
    new MemoryService[K, V] {
      override def get(k: K): Option[V] = source.fn(k)
    }
}
