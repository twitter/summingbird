package com.twitter.summingbird.storm.collector

import backtype.storm.spout.SpoutOutputCollector
import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.FutureQueue
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.{
  SummerBuilder,
  MaxWaitingFutures,
  MaxFutureWaitTime,
  MaxEmitPerExecute
}
import com.twitter.algebird.util.summer.{ AsyncSummer, Incrementor }
import com.twitter.util.{ Await, Duration, Future, Time }
import java.util.{ List => JList }
import scala.collection.mutable.{ ListBuffer, Map => MMap, MutableList => MList }
import scala.collection.{ Map => CMap }
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success }

/**
 *
 * AggregatorOutputCollector is a wrapper around the SpoutOutputCollector.
 * AsyncSummer is used to aggregate the tuples.
 * Different streams have separate aggregators and caches.
 */
class AggregatorOutputCollector[K, V: Semigroup](
    in: SpoutOutputCollector,
    summerBuilder: SummerBuilder,
    maxWaitingFutures: MaxWaitingFutures,
    maxWaitingTime: MaxFutureWaitTime,
    maxEmitPerExec: MaxEmitPerExecute,
    summerShards: KeyValueShards,
    flushExecTimeCounter: Incrementor,
    executeTimeCounter: Incrementor,
    failHandler: AnyRef => Unit) extends SpoutOutputCollector(in) {

  type AggKey = Int
  type AggValue = CMap[K, V]
  type OutputTuple = (AggKey, AggValue)
  type OutputMessageId = TraversableOnce[Object]

  // An individual summer is created for each stream of data. This map keeps track of the stream and its corresponding summer.
  private val cacheByStreamId = MMap.empty[String, AsyncSummer[(K, (Seq[Object], V)), Map[K, (Seq[Object], V)]]]

  private val futureQueueByStreamId = MMap.empty[String, FutureQueue[OutputMessageId, OutputTuple]]

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

  private def convertToSummerInputFormat(flushedCache: CMap[K, (Seq[Object], V)]): TraversableOnce[(OutputMessageId, Future[OutputTuple])] =
    flushedCache.groupBy {
      case (k, _) => summerShards.summerIdFor(k)
    }.map {
      case (index: Int, m: CMap[K, (Seq[Object], V)]) =>
        val messageIds = m.values.flatMap { case (ids, _) => ids }
        val results = m.mapValues { case (_, v) => v }
        (messageIds, Future.value((index, results)))
    }

  /*
    The method is invoked to handle the flushed cache caused by
    exceeding the memoryLimit, which is called within add method.
   */
  private def emitData(
    tuples: Future[TraversableOnce[(OutputMessageId, Future[OutputTuple])]],
    streamId: String): JList[Integer] = {
    val startTime = Time.now
    val futureQueue = futureQueueByStreamId.getOrElseUpdate(
      streamId,
      new FutureQueue(maxWaitingFutures, maxWaitingTime, maxEmitPerExec)
    )
    futureQueue.addAllFuture(Nil, tuples)

    val flushedTups = futureQueue.dequeue
    val result = new java.util.ArrayList[Integer]()
    flushedTups.foreach {
      case (messageIds, t) =>
        t match {
          case Success((groupKey, data)) =>
            val list = new java.util.ArrayList[AnyRef](2)
            list.add(groupKey.asInstanceOf[AnyRef])
            list.add(data.asInstanceOf[AnyRef])
            val emitResult = callEmit(messageIds, list, streamId)
            if (emitResult != null) result.addAll(emitResult)
          case Failure(_) =>
            failHandler(messageIds.asInstanceOf[AnyRef])
        }
    }
    executeTimeCounter.incrBy(Time.now.inMillis - startTime.inMillis)
    result
  }

  /*
   This is a wrapper method to call the emit with appropriate signature
   based on the arguments.
  */
  private def callEmit(messageIds: TraversableOnce[AnyRef], list: JList[AnyRef], stream: String): JList[Integer] = {
    (messageIds.isEmpty, stream.isEmpty) match {
      case (true, true) => in.emit(list)
      case (true, false) => in.emit(stream, list)
      case (false, true) => in.emit(list, messageIds)
      case (false, false) => in.emit(stream, list, messageIds)
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
