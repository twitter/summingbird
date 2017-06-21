package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.storm.builder.Topology

/**
 * This trait describes how to build topology component which corresponds to
 * [[com.twitter.summingbird.Producer]]'s chain (like `flatMap(...).flatMap(...)` or
 * `sumByKey(...)` or even `Source(...).flatMap(...)`).
 */
private[storm] trait ComponentProvider {
  import StormTopologyBuilder._

  /**
   * Create [[Topology.Component]] which emits single events of corresponding type, i.e.
   * if your component corresponds to [[ Producer[Storm, T] ]] and passed function was `identity`
   * your component should emit [[ Item[T] ]] values.
   */
  def createSingle[T, O](fn: Item[T] => O): Topology.Component[O]

  /**
   * Assuming this component corresponds to `Producer[Storm, (K, V)]` (which means it emits `Item[(K, V)]`)
   * and it emits tuples to `sumByKey` with `batcher`, `shards` and `semigroup` parameters
   * this method should return Topology's component which emits aggregated over this parameters
   * `(K, V)` tuples if possible.
   */
  /**
   * Create [[Topology.Component]] which emits aggregated key value events of corresponding type, i.e.
   * if your component corresponds to [[ Summer[Storm, K, V] ]] it will emit [[ Aggregated[K, V] ]].
   * Return [[None]] in case if this operation is unsupported (i.e. component cannot return aggregated values).
   */
  def createAggregated[K, V](
    batcher: Batcher,
    shards: KeyValueShards,
    semigroup: Semigroup[V]
  ): Option[Topology.Component[Aggregated[K, V]]]
}
