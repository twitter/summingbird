package com.twitter.summingbird.storm.collector

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.SummerBuilder
import backtype.storm.spout.SpoutOutputCollector
import com.twitter.algebird.util.summer.AsyncSummer
import com.twitter.util.{ Await, Future }
import java.util.{ List => JList }
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.{ MutableList => MList }
import scala.collection.{ Map => CMap }
import scala.collection.JavaConverters._
import java.util.{ List => JList }

class AggregatorOutputCollector[K, V: Semigroup](in: SpoutOutputCollector, func: JList[AnyRef] => JList[AnyRef], summerBuilder: SummerBuilder, summerShards: KeyValueShards) extends TransformingOutputCollector(in, func) {

  var spoutCaches = MMap[String, AsyncSummer[(K, V), Map[K, V]]]()
  var lastDump = System.currentTimeMillis()
  var streamMessageIdTracker = MMap[String, MMap[Int, MList[Object]]]()

  def timerFlush() = {
    spoutCaches.keys.foreach { stream =>
      val tupsOut = spoutCaches(stream).tick.map { convertToSummerInputFormat(_) }
      val tups = Await.result(tupsOut)

      tups.foreach {
        case (k, v) => {
          val messageIdsTracker = streamMessageIdTracker(stream)
          val messageIds = messageIdsTracker.remove(k)
          (messageIds.isEmpty, stream.isEmpty) match {
            case (true, true) => in.emit(List(k, v).asJava.asInstanceOf[JList[AnyRef]])
            case (true, false) => in.emit(stream, List(k, v).asJava.asInstanceOf[JList[AnyRef]])
            case (false, true) => in.emit(List(k, v).asJava.asInstanceOf[JList[AnyRef]], messageIds)
            case (false, false) => in.emit(stream, List(k, v).asJava.asInstanceOf[JList[AnyRef]], messageIds)
          }
        }
      }
    }
  }

  private def convertToSummerInputFormat(data: CMap[K, V]): CMap[Int, CMap[K, V]] = {
    data.groupBy { case (k, _) => summerShards.summerIdFor(k) }
  }

  private def emitData[K, V](data: Future[Traversable[(Int, CMap[K, V])]], callerfunc: (String, JList[AnyRef], scala.Any) => JList[Integer], s: String, o: scala.Any): List[Int] = {
    var returns = MList[Int]()
    val data_trav = Await.result(data)
    data_trav
      .foreach {
        case (k, v) => {
          val messageIdsTracker = streamMessageIdTracker(s)
          val messageIds = messageIdsTracker.remove(k)
          val taskIds = callerfunc(s, List(k, v).asJava.asInstanceOf[JList[AnyRef]], messageIds)
          if (taskIds != null) returns ++= taskIds.asInstanceOf[JList[Int]].asScala.toList
        }
      }
    returns.toList
  }

  private def add(tuple: (K, V), o: scala.Any, s: String) = {
    if (o != Nil) trackMessageId(tuple, o, s)
    addToCache(tuple, s)
  }

  private def addToCache(tuple: (K, V), s: String) = {
    val cache = spoutCaches.get(s)
    cache match {
      case Some(cac) => cac.add(tuple)
      case None => {
        spoutCaches(s) = summerBuilder.getSummer[K, V](implicitly[Semigroup[V]])
        spoutCaches(s).add(tuple)
      }
    }
  }

  private def trackMessageId(tuple: (K, V), o: scala.Any, s: String): Unit = {
    val messageIdTracker = streamMessageIdTracker.getOrElse(s, MMap[Int, MList[Object]]())
    var messageIds = messageIdTracker.getOrElse(summerShards.summerIdFor(tuple._1), MList())
    messageIdTracker(summerShards.summerIdFor(tuple._1)) = (messageIds.+=(o.asInstanceOf[Object]))
    streamMessageIdTracker(s) = messageIdTracker
  }

  def emitHelper(s: String, list: JList[AnyRef], o: scala.Any)(callerFunc: (String, JList[AnyRef], scala.Any) => JList[Integer]): JList[Integer] = {

    val first: K = func(list).get(0).asInstanceOf[K]
    val second: V = func(list).get(1).asInstanceOf[V]

    val emitReturn = emitData(add((first, second), o, s).map(convertToSummerInputFormat(_)), callerFunc, s, o)
    emitReturn.asJava.asInstanceOf[JList[Integer]]
  }

  override def emit(s: String, list: JList[AnyRef], o: scala.AnyRef): JList[Integer] = emitHelper(s, list, o)((s, l, o) => in.emit(s, l, o))

  override def emit(list: JList[AnyRef], o: scala.AnyRef): JList[Integer] = emitHelper("", list, o)((s, l, o) => in.emit(l, o))

  override def emit(list: JList[AnyRef]): JList[Integer] = emitHelper("", list, Nil)((s, l, o) => in.emit(l))

  override def emit(s: String, list: JList[AnyRef]): JList[Integer] = emitHelper(s, list, Nil)((s, l, o) => in.emit(s, l))

  override def reportError(throwable: Throwable): Unit = in.reportError(throwable)

}
