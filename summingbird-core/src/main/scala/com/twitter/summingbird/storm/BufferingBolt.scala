package com.twitter.summingbird.storm

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.IRichBolt

import com.twitter.algebird.{ Monoid, SummingQueue }
import com.twitter.summingbird.util.CacheSize

import java.util.{ Map => JMap }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

abstract class BufferingBolt[T: Monoid](cacheSize: CacheSize) extends IRichBolt {
  class Mutex extends java.io.Serializable

  // cacheCount is lazy because the call to "size" generates a random
  // number, and we need this to be different for every instance of
  // flatMapBolt to spread out the emit load on the storm cluster.
  lazy val cacheCount = cacheSize.size

  // TODO: Add a metric of (# events put into the queue) / (# events emitted)
  // Note that this lazy val is only ever realized if cacheCount is defined.

  // TODO: SummingQueue should support a count of zero. If zero, then
  // we just pass through and always return the input.
  lazy val buffer = SummingQueue[T](cacheCount.getOrElse(1))

  val mutex = new Mutex
  var collector: OutputCollector = null

  def toCollector[U](fn: OutputCollector => U): U = mutex.synchronized { fn(collector) }

  override def prepare(stormConf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    // There is no need for a mutex here because this called once on start
    collector = oc
  }

  override val getComponentConfiguration = null
}
