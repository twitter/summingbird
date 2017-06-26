package com.twitter.summingbird.storm.builder

import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, SummerBuilder }
import com.twitter.tormenta.spout.SpoutProxy
import com.twitter.util.{ Duration, Time }
import java.util.{ Map => JMap }
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.{ IRichSpout, OutputFieldsDeclarer }
import scala.collection.{ Map => CMap }

/**
 * This is a spout used when the spout is being followed by summer.
 * It uses a AggregatorOutputCollector on open.
 */
private[builder] class KeyValueSpout[K, V: Semigroup](
  protected val self: IRichSpout,
  summerBuilder: SummerBuilder,
  maxEmitPerExec: MaxEmitPerExecute,
  summerShards: KeyValueShards,
  flushExecTimeCounter: Incrementor,
  executeTimeCounter: Incrementor,
  format: OutputFormat[(Int, CMap[K, V])]
) extends SpoutProxy {

  private val tickFrequency = Duration.fromMilliseconds(1000)

  private var adapterCollector: AggregatorOutputCollector[K, V] = _
  var lastDump = Time.now

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit =
    declarer.declare(format.asStormFields)

  /**
   * On open the outputCollector is wrapped with AggregateOutputCollector and fed to the KeyValueSpout.
   */
  override def open(conf: JMap[_, _],
    topologyContext: TopologyContext,
    outputCollector: SpoutOutputCollector): Unit = {
    adapterCollector = new AggregatorOutputCollector(
      outputCollector,
      summerBuilder,
      maxEmitPerExec,
      summerShards,
      flushExecTimeCounter,
      executeTimeCounter,
      format
    )
    super.open(conf, topologyContext, adapterCollector)
  }

  /**
   * This method is used to call the tick on the cache.
   */
  override def nextTuple(): Unit = {
    if (Time.now - lastDump > tickFrequency) {
      adapterCollector.timerFlush()
      lastDump = Time.now
    }
    super.nextTuple()
  }

  /**
   * The AggregateOutputCollectors send the ack on list of messageIds
   * which are crushedDown together. So this msgId is a list of individual messageIds.
   */
  override def ack(msgId: AnyRef): Unit = {
    val msgIds = msgId.asInstanceOf[TraversableOnce[AnyRef]]
    msgIds.foreach { super.ack(_) }
  }

  /**
   * The AggregateOutputCollectors send the fail on list of messageIds
   * which are crushedDown together. So this msgId is a list of individual messageIds.
   */
  override def fail(msgId: AnyRef): Unit = {
    val msgIds = msgId.asInstanceOf[TraversableOnce[AnyRef]]
    msgIds.foreach { super.fail(_) }
  }
}
