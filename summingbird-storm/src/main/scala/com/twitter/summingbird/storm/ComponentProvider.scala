package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.storm.builder.Topology

/**
 * This trait describes how to build topology component which corresponds to `Producer`s chain.
 */
private[storm] trait ComponentProvider {
  import StormTopologyBuilder._

  /**
   * Assuming this component corresponds to `Producer[Storm, T]` (which means it emits `Item[T]`)
   * and function from `Item[T]` to `O` this method should return
   * Topology's component which emits `O` tuples.
   */
  def createSingle[T, O](fn: Item[T] => O): Topology.Component[O]

  /**
   * Assuming this component corresponds to `Producer[Storm, (K, V)]` (which means it emits `Item[(K, V)]`)
   * and it emits tuples to `sumByKey` with `batcher`, `shards` and `semigroup` parameters
   * this method should return Topology's component which emits aggregated over this parameters
   * `(K, V)` tuples if possible.
   */
  def createAggregated[K, V](
    batcher: Batcher,
    shards: KeyValueShards,
    semigroup: Semigroup[V]
  ): Option[Topology.Component[Aggregated[K, V]]]
}
