package com.twitter.summingbird.storm

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.MemoryTestExecutor.{ SinkContent, TestResult }
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.{ PlatformTransformer, TailProducer, TestPlatform, TimeExtractor }
import com.twitter.summingbird.online.{ MergeableStoreFactory, OnlineServiceFactory, ReadableServiceFactory }
import com.twitter.summingbird.storm.Storm.toStormSource
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.util.Future
import java.util.UUID
import scala.collection.mutable

object StormTestExecutor {
  private[storm] val sinks = mutable.Map[UUID, SinkContent[_]]()

  def apply[T](producer: TailProducer[TestPlatform, T], storm: Storm): TestResult = {
    val transformer = new TestToStormTranformer()
    val stormProducer = PlatformTransformer(transformer, producer).asInstanceOf[TailProducer[Storm, T]]
    StormTestRun(stormProducer)(storm)
    val storesResult = transformer.stores
      .toMap
      .mapValues(idWithFactory => TestStore(idWithFactory._1).get.toScala)
    val result = TestResult(storesResult, transformer.getSinks)
    transformer.clearSinks()
    result
  }
}

private class TestToStormTranformer extends PlatformTransformer[TestPlatform, Storm] {
  val stores: mutable.Map[String, (String, MergeableStoreFactory[(_, BatchID), _])] = mutable.Map()
  val sinksUuids: mutable.Map[String, UUID] = mutable.Map()

  override def transformSource[T](source: TestPlatform.SourceList[T]): StormSource[T] = {
    implicit def extractor[E]: TimeExtractor[E] = TimeExtractor(_ => 0L)
    toStormSource(TraversableSpout(source.data))
  }

  override def transformSink[T](source: TestPlatform.Sink[T]): StormSink[T] =
    StormTestExecutor.sinks.synchronized {
      val uuid = sinksUuids.getOrElseUpdate(source.id, UUID.randomUUID())
      new SinkFn[T](v => StormTestExecutor.sinks.synchronized {
        val sink = StormTestExecutor.sinks.getOrElseUpdate(uuid, {
          new SinkContent[T]()
        }).asInstanceOf[SinkContent[T]]
        sink.add(v)
        Future.Unit
      })
    }

  override def transformStore[K, V](source: TestPlatform.Store[K, V]): MergeableStoreFactory[(K, BatchID), V] =
    stores.getOrElseUpdate(source.id, {
      TestStore.createStore(source.initial)(source.valueSemigroup)
        .asInstanceOf[(String, MergeableStoreFactory[(_, BatchID), _])]
    })._2.asInstanceOf[MergeableStoreFactory[(K, BatchID), V]]

  override def transformService[K, V](source: TestPlatform.Service[K, V]): OnlineServiceFactory[K, V] =
    ReadableServiceFactory[K, V](() => ReadableStore.fromFn(source.fn))

  def getSinks: Map[String, List[_]] = StormTestExecutor.sinks.synchronized(
    sinksUuids.mapValues(StormTestExecutor.sinks.get(_).get.toList).toMap)

  def clearSinks(): Unit = StormTestExecutor.sinks.synchronized(
    sinksUuids.values.foreach(uuid => StormTestExecutor.sinks.remove(uuid)))
}
