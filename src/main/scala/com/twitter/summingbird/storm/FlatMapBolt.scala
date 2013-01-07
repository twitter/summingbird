package com.twitter.summingbird.storm

import backtype.storm.topology.base._
import backtype.storm.topology._
import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.tuple._

import com.twitter.algebird.{ MapMonoid, Monoid, SummingQueue }
import com.twitter.chill.MeatLocker
import com.twitter.summingbird.{ Constants, FlatMapper }
import com.twitter.summingbird.util.CacheSize
import com.twitter.summingbird.batch.{ Batcher, BatchID }

import scala.util.Random

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class ObjectMutex extends java.io.Serializable

class FlatMapBolt[Event,Time,Key,Value](@transient fm: FlatMapper[Event,Key,Value], cacheSize: CacheSize)
(implicit monoid: Monoid[Value], batcher: Batcher[Time]) extends IRichBolt {
  import Constants._

  // This is lazy because the call to "size" generates a random
  // number, and we need this to be different for every instance of
  // flatMapBolt to spread out the emit load on the storm cluster.
  lazy val cacheCount = cacheSize.size
  val mapMonoid = new MapMonoid[(Key,BatchID),Value]
  val flatMapBox = MeatLocker(fm)

  // TODO: Add a metric of (# events put into the queue) / (# events emitted)
  // Note that this lazy val is only ever realized if cacheCount is defined.
  lazy val queue = SummingQueue[Map[(Key,BatchID),Value]](cacheCount.getOrElse(1))
  val collectorMutex = new ObjectMutex
  var collector: OutputCollector = null

  // TODO: Once this issue's fixed
  // (https://github.com/twitter/tormenta/issues/1), emit scala
  // tuples directly.

  protected def emitMap(m: Map[(Key,BatchID),Value]) {
    collectorMutex synchronized {
      m foreach { case ((k,batchID),v) =>
        collector.emit(new Values(batchID.asInstanceOf[AnyRef], // BatchID
                                  k.asInstanceOf[AnyRef], // Key
                                  v.asInstanceOf[AnyRef])) // Value
      }
    }
  }

  override def prepare(stormConf: java.util.Map[_,_], context: TopologyContext, oc: OutputCollector) {
    collectorMutex synchronized { collector = oc }
  }

  protected def ack(tuple: Tuple) {
    collectorMutex synchronized { collector.ack(tuple) }
  }

  def cachedExecute(pairs: TraversableOnce[(Key,Value)], batchID: BatchID) {
    val pairMaps: TraversableOnce[Map[(Key,BatchID),Value]] = pairs flatMap { case (k,v) => queue( Map((k, batchID) -> v) ) }
    // sum all options to one option, break out of option, emit all.
    emitMap(Monoid.sum(pairMaps))
  }

  def uncachedExecute(pairs: TraversableOnce[(Key,Value)], batchID: BatchID) {
    // TODO: Because the bolt has persistent state, we can have the
    // bolt sample the stream and evaluate if this foldLeft is needed
    // (and remove it if unnecessary).
    val toEmit =
      pairs.foldLeft(mapMonoid.zero){ (acc, pair) =>
        val (k,v) = pair
        mapMonoid.plus(acc, Map((k,batchID) -> v))
      }
    emitMap(toEmit)
  }

  override def execute(tuple: Tuple) {
    // TODO: We need to get types down into the IRichSpout so we can validate this.
    val (event,time) = tuple.getValue(0).asInstanceOf[(Event,Time)]
    val batchID = batcher.batchOf(time)
    val pairs = flatMapBox.get.encode(event)
    if (cacheCount.isDefined)
      cachedExecute(pairs, batchID)
    else
      uncachedExecute(pairs, batchID)

    // TODO: Think about whether we want to ack tuples when they're
    // actually processed, vs always acking them here. This will
    // complicate the logic of the cached case (as we'll have to track
    // the tuple -> kv mapping.
    ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields(AGG_BATCH, AGG_KEY, AGG_VALUE))
  }

  override def cleanup { fm.cleanup }
  override val getComponentConfiguration = null
}
