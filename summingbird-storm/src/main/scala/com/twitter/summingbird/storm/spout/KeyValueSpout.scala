package com.twitter.summingbird.storm.spout

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{ IRichSpout, OutputFieldsDeclarer }
import backtype.storm.tuple.Fields
import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.SummerBuilder
import com.twitter.summingbird.storm.Constants._
import com.twitter.tormenta.spout.SpoutProxy
import java.util
import java.util.{ List => JList }
import scala.collection.mutable.{ MutableList => MList }
import com.twitter.summingbird.storm.collector.AggregatorOutputCollector
import com.twitter.util.{ Duration, Time }

/**
 * This is a spout used when the spout is being followed by summer.
 * It uses a AggregatorOutputCollector on open.
 */
class KeyValueSpout[K, V: Semigroup](val in: IRichSpout, summerBuilder: SummerBuilder, summerShards: KeyValueShards, flushExecTimeCounter: Incrementor, executeTimeCounter: Incrementor) extends SpoutProxy {

  private final val tickFrequency = Duration.fromMilliseconds(1000)
  private var adapterCollector: AggregatorOutputCollector[K, V] = _
  var lastDump = Time.now

  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    declarer.declare(new Fields(AGG_KEY, AGG_VALUE))
  }

  override def open(conf: util.Map[_, _],
    topologyContext: TopologyContext,
    outputCollector: SpoutOutputCollector): Unit = {
    adapterCollector = new AggregatorOutputCollector(outputCollector, summerBuilder, summerShards, flushExecTimeCounter, executeTimeCounter)
    in.open(conf, topologyContext, adapterCollector)
  }

  /**
   * This method is used to call the tick on the cache.
   */
  override def nextTuple(): Unit = {
    if (Time.now - lastDump > tickFrequency) {
      adapterCollector.timerFlush()
      lastDump = Time.now
    }
    in.nextTuple()
  }

  /**
   * The AggregateOutputCollectors send the ack on list of messageIds
   * which are crushedDown together. So this msgId is a list of individual messageIds.
   */
  override def ack(msgId: AnyRef): Unit = {
    val msgIds = convertToList(msgId)
    msgIds.foreach { super.ack(_) }
  }

  /**
   * The AggregateOutputCollectors send the fail on list of messageIds
   * which are crushedDown together. So this msgId is a list of individual messageIds.
   */
  override def fail(msgId: AnyRef): Unit = {
    val msgIds = convertToList(msgId)
    msgIds.foreach { super.fail(_) }
  }

  /**
   * The msgId Object is a list of individual messageIds of all the aggregated tuples.
   */
  private def convertToList(msgId: AnyRef): TraversableOnce[AnyRef] = {
    msgId match {
      case Some(s: TraversableOnce[_]) => s.asInstanceOf[TraversableOnce[AnyRef]]
      case None => Nil
      case otherwise => throw new ClassCastException(s"expected Option[TraversableOnce[AnyRef]] found class: ${otherwise.getClass} with value: $otherwise")
    }
  }

  override protected def self: IRichSpout = in
}
