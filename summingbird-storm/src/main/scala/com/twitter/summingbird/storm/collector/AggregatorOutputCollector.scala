package com.twitter.summingbird.storm.collector

import backtype.storm.spout.SpoutOutputCollector
import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.SummerBuilder
import com.twitter.algebird.util.summer.{ AsyncSummer, Incrementor }
import com.twitter.util.{ Await, Future, Time }
import java.util.{ List => JList }
import scala.collection.mutable.{ ListBuffer, Map => MMap }
import scala.collection.JavaConverters._

/**
 *
 * AggregatorOutputCollector is a wrapper around the SpoutOutputCollector.
 * AsyncSummer is used to aggregate the tuples.
 * Different streams have separate aggregators and caches.
 */
class AggregatorOutputCollector[K, V: Semigroup](
    in: SpoutOutputCollector,
    summerBuilder: SummerBuilder,
    summerShards: KeyValueShards,
    flushExecTimeCounter: Incrementor,
    executeTimeCounter: Incrementor) extends SpoutOutputCollector(in) {

  private type AggKey = Int
  private type AggValue = Map[K, V]
  private type OutputMessageId = TraversableOnce[Object]
  private type OutputTuple = (AggKey, AggValue, OutputMessageId)

  // An individual summer is created for each stream of data. This map keeps track of the stream and its corresponding summer.
  private val cacheByStreamId = MMap.empty[String, AsyncSummer[(K, (Seq[Object], V)), Map[K, (Seq[Object], V)]]]

  /**
   * This method is invoked from the nextTuple() of the spout.
   * This is triggered with tick frequency of the spout.
   */
  def timerFlush(): Unit = {
    val startTime = Time.now
    cacheByStreamId.foreach {
      case (stream, cache) =>
        val tupsOut = cache.tick.map { convertToSummerInputFormat(_) }
        emitData(tupsOut, stream)
    }
    flushExecTimeCounter.incrBy(Time.now.inMillis - startTime.inMillis)
  }

  private def convertToSummerInputFormat(flushedCache: Map[K, (Seq[Object], V)]): TraversableOnce[OutputTuple] =
    flushedCache.groupBy {
      case (k, _) => summerShards.summerIdFor(k)
    }.map {
      case (index: AggKey, m: Map[K, (OutputMessageId, V)]) =>
        val messageIds = m.values.flatMap { case (ids, _) => ids }
        val results = m.mapValues { case (_, v) => v }
        (index, results, messageIds)
    }

  /*
    The method is invoked to handle the flushed cache caused by
    exceeding the memoryLimit, which is called within add method.
   */
  private def emitData(tuples: Future[TraversableOnce[OutputTuple]], streamId: String): JList[Integer] = {
    val startTime = Time.now

    val flushedTups = Await.result(tuples)
    val result = new java.util.ArrayList[Integer]()
    flushedTups.foreach {
      case (groupKey, data, messageIds) =>
        val tuple = new java.util.ArrayList[AnyRef](2)
        tuple.add(groupKey.asInstanceOf[AnyRef])
        tuple.add(data.asInstanceOf[AnyRef])
        val emitResult = callEmit(tuple, messageIds, streamId)
        if (emitResult != null) result.addAll(emitResult)
    }
    executeTimeCounter.incrBy(Time.now.inMillis - startTime.inMillis)
    result
  }

  /*
   This is a wrapper method to call the emit with appropriate signature
   based on the arguments.
  */
  private def callEmit(tuple: JList[AnyRef], messageIds: TraversableOnce[AnyRef], stream: String): JList[Integer] = {
    (messageIds.isEmpty, stream.isEmpty) match {
      case (true, true) => in.emit(tuple)
      case (true, false) => in.emit(stream, tuple)
      case (false, true) => in.emit(tuple, messageIds)
      case (false, false) => in.emit(stream, tuple, messageIds)
    }
  }

  /*
  Method wraps the adding the tuple to the spoutCache along with adding the corresponding
  messageId to the messageId Tracker.
   */
  private def add(tuple: (K, V), streamId: String, messageId: Option[AnyRef] = None): Future[Map[K, (Seq[Object], V)]] = {
    val buffer = messageId.map { id => ListBuffer(id) }.getOrElse(Nil)
    val (k, v) = tuple
    cacheByStreamId.getOrElseUpdate(streamId, summerBuilder.getSummer[K, (Seq[Object], V)](implicitly[Semigroup[(Seq[Object], V)]]))
      .add(k -> ((buffer, v)))
  }

  private def extractAndProcessElements(streamId: String, list: JList[AnyRef], messageId: Option[AnyRef] = None): JList[Integer] = {
    val tupleKV = list.get(0).asInstanceOf[(K, V)]
    emitData(add(tupleKV, streamId, messageId).map(convertToSummerInputFormat(_)), streamId)
  }

  override def emit(s: String, list: JList[AnyRef], o: AnyRef): JList[Integer] = extractAndProcessElements(s, list, Some(o))

  override def emit(list: JList[AnyRef], o: AnyRef): JList[Integer] = extractAndProcessElements("", list, Some(o))

  override def emit(list: JList[AnyRef]): JList[Integer] = extractAndProcessElements("", list)

  override def emit(s: String, list: JList[AnyRef]): JList[Integer] = extractAndProcessElements(s, list)

  override def reportError(throwable: Throwable): Unit = in.reportError(throwable)
}
