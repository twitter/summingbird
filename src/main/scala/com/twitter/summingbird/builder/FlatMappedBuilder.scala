package com.twitter.summingbird.builder

import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.BoltDeclarer
import cascading.flow.FlowDef
import com.twitter.bijection.Bijection
import com.twitter.chill.BijectionPair
import com.twitter.scalding.TypedPipe
import com.twitter.summingbird.Env
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.storm.StormEnv
import com.twitter.summingbird.sink.CompoundSink

import java.io.Serializable

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * The flatmapped builder represents an EventSource with a FlatMapper attached.
 * A FlatMappedBuilder can be added to other flatmapped builders with the same
 * key, value and time types.
 */

abstract class FlatMappedBuilder[Time: Manifest, Key: Manifest, Value: Manifest] extends Serializable {
  // Sources of many event types might create the same key-value pairs, so we use a List.
  val eventCodecPairs: List[BijectionPair[_]]

  // Necessary to expose those for serialization registration.
  val timeClass: Class[Time] = manifest[Time].erasure.asInstanceOf[Class[Time]]
  val keyClass: Class[Key] = manifest[Key].erasure.asInstanceOf[Class[Key]]
  val valueClass: Class[Value] = manifest[Value].erasure.asInstanceOf[Class[Value]]

  // This allows you to build a storm topology / scalding job
  def groupAndSumTo(newsink: CompoundSink[Time,Key,Value])
  (implicit env: Env, keyCodec: Bijection[Key,Array[Byte]], valCodec: Bijection[Value,Array[Byte]])
  :CompletedBuilder[Time,Key,Value] = {
    val cb = new CompletedBuilder(this, newsink, keyCodec, valCodec)
    env.builder = cb
    cb
  }

  // useful when you need to merge two different Event sources
  def ++(other: FlatMappedBuilder[Time,Key,Value]): FlatMappedBuilder[Time,Key,Value] =
    new MergedFlatMappedBuilder(this, other)

  def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String): Unit
  def attach(groupBySumBolt: BoltDeclarer, suffix: String): BoltDeclarer
  def getFlatMappedPipe(batcher: Batcher[Time], lowerb: BatchID, env: ScaldingEnv)(implicit fd: FlowDef): TypedPipe[(Time,Key,Value)]
  def keyOrdering: Ordering[Key]
}
