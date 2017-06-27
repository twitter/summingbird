package com.twitter.summingbird.storm

import com.twitter.bijection.{ Injection, Inversion }
import com.twitter.summingbird.storm.builder.{ EdgeGrouping, OutputFormat, Topology }
import scala.util.Try
import java.util.{ ArrayList => JAList, List => JList }
import StormTopologyBuilder._
import com.twitter.summingbird.batch.Timestamp

/**
 * This companion object contains all [[Topology.Edge]]'s
 * types used by Summingbird's storm topology.
 *
 * @see [[StormTopologyBuilder]] for different types of values used here.
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

  def groupedKeyValueItemToItem[K, V](
    sourceId: Topology.EmittingId[Item[(K, V)]],
    destId: Topology.ReceivingId[Item[(K, V)]]
  ): Topology.Edge[Item[(K, V)], Item[(K, V)]] = Topology.Edge(
    sourceId,
    EdgeFormats.keyValue[K, V],
    EdgeGrouping.Fields(List(EdgeFormats.Key)),
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

  def groupedShardedToItem[K, V](
    sourceId: Topology.EmittingId[Sharded[K, V]],
    destId: Topology.ReceivingId[Item[(K, V)]]
  ): Topology.Edge[Sharded[K, V], Item[(K, V)]] = Topology.Edge(
    sourceId,
    EdgeFormats.sharded[K, V],
    EdgeGrouping.Fields(List(EdgeFormats.ShardKey)),
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
  val Key = "key"
  val ShardKey = "shard"

  def item[T]: OutputFormat[Item[T]] =
    OutputFormat(List("timestampWithValue"), EdgeInjections.Single())
  def aggregated[K, V]: OutputFormat[Aggregated[K, V]] =
    OutputFormat(List(ShardKey, "aggregated"), EdgeInjections.Pair())
  def keyValue[K, V]: OutputFormat[Item[(K, V)]] =
    OutputFormat(List("timestamp", Key, "value"), EdgeInjections.KeyValueInjection())
  def sharded[K, V]: OutputFormat[Sharded[K, V]] =
    OutputFormat(List(ShardKey, "keyWithBatch", "valueWithTimestamp"), EdgeInjections.Triple())
}

private object EdgeInjections {
  case class Single[T]() extends Injection[T, JList[AnyRef]] {
    override def apply(tuple: T): JAList[AnyRef] = {
      val list = new JAList[AnyRef](1)
      list.add(tuple.asInstanceOf[AnyRef])
      list
    }

    override def invert(valueIn: JList[AnyRef]): Try[T] = Inversion.attempt(valueIn) { input =>
      input.get(0).asInstanceOf[T]
    }
  }

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

  case class KeyValueInjection[K, V]() extends Injection[Item[(K, V)], JList[AnyRef]] {
    override def apply(tuple: Item[(K, V)]): JList[AnyRef] = tuple match {
      case (timestamp, (key, value)) =>
        val list = new JAList[AnyRef](3)
        list.add(timestamp.asInstanceOf[AnyRef])
        list.add(key.asInstanceOf[AnyRef])
        list.add(value.asInstanceOf[AnyRef])
        list
    }

    override def invert(valueIn: JList[AnyRef]): Try[Item[(K, V)]] = Inversion.attempt(valueIn) { input =>
      val timestamp = input.get(0).asInstanceOf[Timestamp]
      val key = input.get(1).asInstanceOf[K]
      val value = input.get(2).asInstanceOf[V]
      (timestamp, (key, value))
    }
  }
}
