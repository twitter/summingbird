package com.twitter.summingbird.builder

import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.BoltDeclarer

import cascading.flow.FlowDef

import com.twitter.scalding.TypedPipe

import com.twitter.summingbird.batch.{ Batcher, BatchID }

import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.storm.StormEnv

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * MergedFlatMappedBuilder describes the union of two individual FlatMappedBuilders.
 */

class MergedFlatMappedBuilder[Time: Manifest,Key: Manifest,Value: Manifest]
(left: FlatMappedBuilder[Time,Key,Value],
 right: FlatMappedBuilder[Time,Key,Value])
extends FlatMappedBuilder[Time,Key,Value] {

  override lazy val eventCodecPairs = left.eventCodecPairs ++ right.eventCodecPairs

  override def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String) {
    left.addToTopo(env, tb, suffix + "-L")
    right.addToTopo(env, tb, suffix + "-R")
  }

  override def attach(groupBySumBolt: BoltDeclarer, suffix: String) = {
    val ldec = left.attach(groupBySumBolt, suffix + "-L")
    right.attach(ldec, suffix + "-R")
  }

  override def getFlatMappedPipe(batcher: Batcher[Time], lowerb: BatchID, env: ScaldingEnv)
  (implicit fd : FlowDef): TypedPipe[(Time,Key,Value)] = {
    left.getFlatMappedPipe(batcher, lowerb, env) ++ right.getFlatMappedPipe(batcher, lowerb, env)
  }

  override def keyOrdering = left.keyOrdering
}
