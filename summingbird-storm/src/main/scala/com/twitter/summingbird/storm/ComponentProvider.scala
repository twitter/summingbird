package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.storm.builder.Topology

private[storm] trait ComponentProvider {
  import StormTopologyBuilder._

  def createSingle[T, O](fn: Item[T] => O): Topology.Component[O]

  def createAggregated[K, V](
    batcher: Batcher,
    shards: KeyValueShards,
    semigroup: Semigroup[V]
  ): Option[Topology.Component[Aggregated[K, V]]]
}
