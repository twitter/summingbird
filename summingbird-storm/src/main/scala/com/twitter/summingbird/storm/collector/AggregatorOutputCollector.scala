package com.twitter.summingbird.storm.collector

import backtype.storm.spout.SpoutOutputCollector
import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.FutureQueue
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.{ SummerBuilder, MaxWaitingFutures, MaxFutureWaitTime, MaxEmitPerExecute }
import com.twitter.algebird.util.summer.{ AsyncSummer, Incrementor }
import com.twitter.util.{ Future, Return, Throw, Time }
import java.util.{ List => JList }
import scala.collection.{ Map => CMap }
import scala.collection.mutable.{ Map => MMap }
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
    executeTimeCounter: Incrementor) extends SpoutOutputCollector(in) {

  private type AggKey = Int
  private type AggValue = CMap[K, V]
  private type OutputMessageId = Iterator[Object]
  private type OutputTuple = (AggKey, AggValue)

  // An individual summer is created for each stream of data. This map keeps track of the stream and its corresponding summer.
  private val cacheByStreamId = MMap.empty[String, AsyncSummer[(K, (OutputMessageId, V)), Map[K, (OutputMessageId, V)]]]

  private val futureQueueByStreamId = MMap.empty[String, FutureQueue[OutputMessageId, OutputTuple]]

  implicit val messageIdSg = new Semigroup[OutputMessageId] {
    override def plus(a: OutputMessageId, b: OutputMessageId) = a ++ b
    override def sumOption(iter: TraversableOnce[OutputMessageId]) =
      if (iter.isEmpty) None else Some(iter.flatten)
  }

  private def getSummer = summerBuilder.getSummer[K, (OutputMessageId, V)]

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

  private def convertToSummerInputFormat(flushedCache: CMap[K, (OutputMessageId, V)]): TraversableOnce[(OutputMessageId, Future[OutputTuple])] =
    flushedCache.groupBy {
      case (k, _) => summerShards.summerIdFor(k)
    }.iterator.map {
      case (index, data) =>
        val messageIds = data.values.iterator.flatMap { case (ids, _) => ids }
        val results = data.mapValues { case (_, v) => v }
        (messageIds, Future.value((index, results)))
    }

  /*
    The method is invoked to handle the flushed cache caused by
    exceeding the memoryLimit, which is called within add method.
   */
  private def emitData(tuples: Future[TraversableOnce[(OutputMessageId, Future[OutputTuple])]], streamId: String): JList[Integer] = {
    val startTime = Time.now
    val futureQueue = futureQueueByStreamId.getOrElseUpdate(
      streamId,
      new FutureQueue(maxWaitingFutures, maxWaitingTime)
    )
    tuples.respond {
      case Return(iter) => futureQueue.addAll(iter)
      case Throw(ex) => futureQueue.add(Iterator.empty, Future.exception(ex))
    }

    val flushedTups = futureQueue.dequeue(maxEmitPerExec.get)
    val result = new java.util.ArrayList[Integer]()
    flushedTups.foreach {
      case (messageIds, resultTry) =>
        // There's no post-processing after aggregation, so this will always
        // be a success.  See convertToSummerInputFormat construction.
        val (groupKey, data) = resultTry.get
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
  private def add(tuple: (K, V), streamId: String, messageId: Option[AnyRef] = None): Future[Map[K, (OutputMessageId, V)]] = {
    val buffer = messageId.iterator
    val (k, v) = tuple
    cacheByStreamId.getOrElseUpdate(streamId, getSummer)
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
