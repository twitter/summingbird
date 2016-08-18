package com.twitter.summingbird.storm.collector

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.SummerBuilder
import backtype.storm.spout.SpoutOutputCollector
import com.twitter.algebird.util.summer.{ AsyncSummer, Incrementor }
import com.twitter.util.{ Await, Duration, Future, Time }
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.{ MutableList => MList }
import scala.collection.{ Map => CMap }
import scala.collection.JavaConverters._
import java.util.{ List => JList }

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

  // Mapping from streams to summers
  private val spoutCaches = MMap[String, AsyncSummer[(K, V), Map[K, V]]]()

  // The Map keeps track of aggregated tuples' messageIds. It also has stream level tracking.
  private val cachesByStreamId = MMap[String, MMap[Int, MList[Object]]]()

  def timerFlush(): Unit = {
    /*
    This method is invoked from the nextTuple() of the spout.
    This is triggered with tick frequency of the spout.
     */
    val startTime = Time.now
    spoutCaches.foreach {
      case (stream, cache) =>
        val tupsOut = cache.tick.map { convertToSummerInputFormat(_) }
        emitData(tupsOut, stream)
    }
    flushExecTimeCounter.incrBy(Time.now.inMillis - startTime.inMillis)
  }

  private def convertToSummerInputFormat(flushedCache: CMap[_, _]): CMap[Int, CMap[_, _]] =
    flushedCache.groupBy { case (k, _) => summerShards.summerIdFor(k) }

  /*
    The method is invoked to handle the flushed cache caused by
    exceeding the memoryLimit, which is called within add method.
   */
  private def emitData(cache: Future[TraversableOnce[(Int, CMap[_, _])]], streamId: String): JList[Integer] = {
    val startTime = Time.now
    val flushedTups = Await.result(cache)
    val messageIdsTracker = cachesByStreamId(streamId)
    val returns = flushedTups.map {
      case (k, v) =>
        val messageIds = messageIdsTracker.remove(k)
        val list = new java.util.ArrayList[AnyRef](2)
        list.add(k.asInstanceOf[AnyRef])
        list.add(v.asInstanceOf[AnyRef])
        callEmit(messageIds, list, streamId)
    }
    val result = new java.util.ArrayList[Integer]()
    returns.filter(_ != null).foreach { result.addAll(_) }
    executeTimeCounter.incrBy(Time.now.inMillis - startTime.inMillis)
    result
  }

  /*
   This is a wrapper method to call the emit with appropriate signature
   based on the arguments.
  */
  private def callEmit(messageIds: Option[TraversableOnce[AnyRef]], list: JList[AnyRef], stream: String): JList[Integer] = {
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
  private def add(tuple: (K, V), streamid: String, messageId: Option[AnyRef] = None): Future[Map[K, V]] = {
    if (messageId.isDefined)
      trackMessageId(tuple, messageId.get, streamid)
    addToCache(tuple, streamid)
  }

  private def addToCache(tuple: (K, V), streamid: String): Future[Map[K, V]] = {
    spoutCaches.get(streamid) match {
      case Some(cache) => cache.add(tuple)
      case None => {
        spoutCaches(streamid) = summerBuilder.getSummer[K, V](implicitly[Semigroup[V]])
        spoutCaches(streamid).add(tuple)
      }
    }
  }

  private def trackMessageId(tuple: (K, V), o: AnyRef, s: String): Unit = {
    val messageIdTracker = cachesByStreamId.getOrElse(s, MMap[Int, MList[Object]]())
    var messageIds = messageIdTracker.getOrElse(summerShards.summerIdFor(tuple._1), MList())
    messageIdTracker(summerShards.summerIdFor(tuple._1)) = messageIds += o
    cachesByStreamId(s) = messageIdTracker
  }

  private def extractAndProcessElements(streamId: String, list: JList[AnyRef], messageId: Option[AnyRef] = None): JList[Integer] = {
    val listKV = list.get(0).asInstanceOf[JList[AnyRef]]
    val first: K = listKV.get(0).asInstanceOf[K]
    val second: V = listKV.get(1).asInstanceOf[V]
    emitData(add((first, second), streamId, messageId).map(convertToSummerInputFormat(_)), streamId)
  }

  override def emit(s: String, list: JList[AnyRef], o: AnyRef): JList[Integer] = extractAndProcessElements(s, list, Some(o))

  override def emit(list: JList[AnyRef], o: AnyRef): JList[Integer] = extractAndProcessElements("", list, Some(o))

  override def emit(list: JList[AnyRef]): JList[Integer] = extractAndProcessElements("", list)

  override def emit(s: String, list: JList[AnyRef]): JList[Integer] = extractAndProcessElements(s, list)

  override def reportError(throwable: Throwable): Unit = in.reportError(throwable)
}
