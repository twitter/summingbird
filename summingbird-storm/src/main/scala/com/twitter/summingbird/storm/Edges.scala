package com.twitter.summingbird.storm

import com.twitter.bijection.{ Injection, Inversion }
import com.twitter.summingbird.storm.builder.{ EdgeGrouping, OutputFormat, Topology }
import scala.util.Try
import java.util.{ ArrayList => JAList, List => JList }
import StormTopologyBuilder._

/**
 * This companion object contains all `Edge`s types used by Summingbird's storm topology.
 */
private[storm] object Edges {
  def shuffleItemToItem[T](
    sourceId: Topology.EmittingId[Item[T]],
    destId: Topology.ReceivingId[Item[T]],
    withLocal: Boolean
  ): Topology.Edge[Item[T], Item[T]] = Topology.Edge(
    sourceId,
    EdgeFormats.item[T],
    if (withLocal) EdgeGrouping.LocalOrShuffle else EdgeGrouping.Shuffle,
    identity,
    destId
  )

  def groupedAggregatedToSummer[K, V](
    sourceId: Topology.EmittingId[Aggregated[K, V]],
    destId: Topology.ReceivingId[SummerInput[K, V]]
  ): Topology.Edge[Aggregated[K, V], SummerInput[K, V]] = Topology.Edge(
    sourceId,
    EdgeFormats.aggregated[K, V],
    EdgeGrouping.Fields(List(EdgeFormats.ShardKey)),
    aggregatedToSummerInput[K, V],
    destId
  )

  def shuffleShardedToItem[K, V](
    sourceId: Topology.EmittingId[Sharded[K, V]],
    destId: Topology.ReceivingId[Item[(K, V)]],
    withLocal: Boolean
  ): Topology.Edge[Sharded[K, V], Item[(K, V)]] = Topology.Edge(
    sourceId,
    EdgeFormats.sharded[K, V],
    if (withLocal) EdgeGrouping.LocalOrShuffle else EdgeGrouping.Shuffle,
    shardedToItem[K, V],
    destId
  )

  def groupedShardedToSummer[K, V](
    sourceId: Topology.EmittingId[Sharded[K, V]],
    destId: Topology.ReceivingId[SummerInput[K, V]]
  ): Topology.Edge[Sharded[K, V], SummerInput[K, V]] = Topology.Edge(
    sourceId,
    EdgeFormats.sharded[K, V],
    EdgeGrouping.Fields(List(EdgeFormats.ShardKey)),
    shardedToSummerInput[K, V],
    destId
  )
}

/**
 * This companion object contains all `OutputFormat`s used by Summingbird's storm topology.
 */
private object EdgeFormats {
  val ShardKey = "shard"

  def item[T]: OutputFormat[Item[T]] =
    OutputFormat(List("timestamp", "value"), EdgeInjections.Pair())
  def aggregated[K, V]: OutputFormat[Aggregated[K, V]] =
    OutputFormat(List(ShardKey, "aggregated"), EdgeInjections.Pair())
  def sharded[K, V]: OutputFormat[Sharded[K, V]] =
    OutputFormat(List(ShardKey, "keyWithBatch", "valueWithTimestamp"), EdgeInjections.Triple())
}

private object EdgeInjections {
  case class Pair[T1, T2]() extends Injection[(T1, T2), JList[AnyRef]] {
    override def apply(tuple: (T1, T2)): JAList[AnyRef] = {
      val list = new JAList[AnyRef](2)
      list.add(tuple._1.asInstanceOf[AnyRef])
      list.add(tuple._2.asInstanceOf[AnyRef])
      list
    }

    override def invert(valueIn: JList[AnyRef]): Try[(T1, T2)] = Inversion.attempt(valueIn) { input =>
      (input.get(0).asInstanceOf[T1], input.get(1).asInstanceOf[T2])
    }
  }

  case class Triple[T1, T2, T3]() extends Injection[(T1, T2, T3), JList[AnyRef]] {
    override def apply(tuple: (T1, T2, T3)): JAList[AnyRef] = {
      val list = new JAList[AnyRef](3)
      list.add(tuple._1.asInstanceOf[AnyRef])
      list.add(tuple._2.asInstanceOf[AnyRef])
      list.add(tuple._3.asInstanceOf[AnyRef])
      list
    }

    override def invert(valueIn: JList[AnyRef]): Try[(T1, T2, T3)] = Inversion.attempt(valueIn) { input =>
      (input.get(0).asInstanceOf[T1], input.get(1).asInstanceOf[T2], input.get(2).asInstanceOf[T3])
    }
  }
}
