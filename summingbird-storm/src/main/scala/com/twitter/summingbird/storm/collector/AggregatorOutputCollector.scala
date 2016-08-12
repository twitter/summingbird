package com.twitter.summingbird.storm.collector

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.SummerBuilder
import backtype.storm.spout.SpoutOutputCollector
import com.twitter.algebird.util.summer.AsyncSummer
import com.twitter.util.{ Await, Future, Time }
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.{ MutableList => MList }
import scala.collection.{ Map => CMap }
import scala.collection.JavaConverters._
import java.util.{ List => JList }

/**
 *
 * AggregatorOutputCollector is a wrapper around the SpoutOutputCollector.
 * AsyncSummer is used to aggregate the tuples.
 * Different streams have seperated aggregators and caches.
 *
 */
class AggregatorOutputCollector[K, V: Semigroup](
    in: SpoutOutputCollector,
    summerBuilder: SummerBuilder,
    summerShards: KeyValueShards) extends SpoutOutputCollector(in) {

  // Map keeps track of summers corresponding to streams.
  val spoutCaches = MMap[String, AsyncSummer[(K, V), Map[K, V]]]()

  var lastDump = Time.now.inMillis

  // The Map keeps track of batch of aggregated tuples' messageIds. It also has a stream level tracking.
  val streamMessageIdTracker = MMap[String, MMap[Int, MList[Object]]]()

  def timerFlush() = {
    /*
    This is a flush called from the nextTuple() of the spout.
    The timerFlush is triggered with tick frequency from the spout.
     */
    spoutCaches.foreach {
      case (stream, cache) =>
        val tupsOut = cache.tick.map { convertToSummerInputFormat(_) }
        emitData(tupsOut, stream)
    }
  }

  private def convertToSummerInputFormat(flushedCache: CMap[K, V]): CMap[Int, CMap[K, V]] =
    flushedCache.groupBy { case (k, _) => summerShards.summerIdFor(k) }

  /*
    The method is invoked to handle the flushed cache caused by
    exceeding the memoryLimit, which is called within add method.
   */
  private def emitData[K, V](cache: Future[Traversable[(Int, CMap[K, V])]], streamId: String): List[Int] = {
    val flushedTups = Await.result(cache)
    val messageIdsTracker = streamMessageIdTracker(streamId)
    val returns = flushedTups.toList
      .map {
        case (k, v) =>
          val messageIds = messageIdsTracker.remove(k)
          val list = List(k, v).asJava.asInstanceOf[JList[AnyRef]]
          callEmit(messageIds, list, streamId)
      }
    returns.flatten
  }

  /*
   This is a wrapper method to call the emit with appropriate signature
   based on the arguments.
  */
  private def callEmit(messageIds: Option[Any], list: JList[AnyRef], stream: String): JList[Integer] = {
    (messageIds.isEmpty, stream.isEmpty) match {
      case (true, true) => in.emit(list)
      case (true, false) => in.emit(stream, list)
      case (false, true) => in.emit(list, messageIds)
      case (false, false) => in.emit(stream, list, messageIds)
    }
  }

  private def add(tuple: (K, V), streamid: String, messageId: Option[Any] = None) = {
    if (messageId.isDefined)
      trackMessageId(tuple, messageId.get, streamid)
    addToCache(tuple, streamid)
  }

  private def addToCache(tuple: (K, V), streamid: String) = {
    spoutCaches.get(streamid) match {
      case Some(cac) => cac.add(tuple)
      case None => {
        spoutCaches(streamid) = summerBuilder.getSummer[K, V](implicitly[Semigroup[V]])
        spoutCaches(streamid).add(tuple)
      }
    }
  }

  private def trackMessageId(tuple: (K, V), o: scala.Any, s: String): Unit = {
    val messageIdTracker = streamMessageIdTracker.getOrElse(s, MMap[Int, MList[Object]]())
    var messageIds = messageIdTracker.getOrElse(summerShards.summerIdFor(tuple._1), MList())
    messageIdTracker(summerShards.summerIdFor(tuple._1)) = ( messageIds += o.asInstanceOf[Object] )
    streamMessageIdTracker(s) = messageIdTracker
  }

  def extractAndProcessElements(streamId: String, list: JList[AnyRef], messageId: Option[Any] = None): JList[Integer] = {
    val listKV = list.get(0).asInstanceOf[JList[AnyRef]]
    val first: K = listKV.get(0).asInstanceOf[K]
    val second: V = listKV.get(1).asInstanceOf[V]
    val emitReturn = emitData(add((first, second), streamId, messageId).map(convertToSummerInputFormat(_)), streamId)
    emitReturn.asJava.asInstanceOf[JList[Integer]]
  }

  override def emit(s: String, list: JList[AnyRef], o: scala.AnyRef): JList[Integer] = extractAndProcessElements(s, list, Some(o))

  override def emit(list: JList[AnyRef], o: scala.AnyRef): JList[Integer] = extractAndProcessElements("", list, Some(o))

  override def emit(list: JList[AnyRef]): JList[Integer] = extractAndProcessElements("", list)

  override def emit(s: String, list: JList[AnyRef]): JList[Integer] = extractAndProcessElements(s, list)

  override def reportError(throwable: Throwable): Unit = in.reportError(throwable)
}
