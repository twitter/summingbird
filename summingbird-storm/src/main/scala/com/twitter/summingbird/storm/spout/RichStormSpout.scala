package com.twitter.summingbird.storm.spout

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{ IRichSpout, OutputFieldsDeclarer }
import backtype.storm.tuple.Fields
import com.twitter.summingbird.storm.Constants._
import com.twitter.tormenta.spout.SpoutProxy
import java.util
import java.util.{ List => JList }
import com.twitter.summingbird.storm.collector.KeyValueOutputCollector

/**
 * Created by pnaramsetti on 7/19/16.
 *
 * This is a spout used when the spout is being followed by summer.
 * It uses a KeyValueOutputCollector on open.
 */
class RichStormSpout(val self: IRichSpout) extends SpoutProxy {

  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    declarer.declare(new Fields(AGG_KEY, AGG_VALUE))
  }

  /*
  * The transform is the function which unwraps the Value object to get the actual fields present in it.
  */
  val transform = {
    input: JList[AnyRef] => input.get(0).asInstanceOf[JList[AnyRef]]
  }

  override def open(conf: util.Map[_, _],
    topologyContext: TopologyContext,
    outputCollector: SpoutOutputCollector): Unit = {
    val adapterCollector = new KeyValueOutputCollector(outputCollector, transform)
    self.open(conf, topologyContext, adapterCollector)
  }
}