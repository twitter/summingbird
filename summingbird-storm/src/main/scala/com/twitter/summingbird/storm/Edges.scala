package com.twitter.summingbird.storm

import com.twitter.bijection.{ Injection, Inversion }
import com.twitter.summingbird.storm.builder.{ EdgeGrouping, OutputFormat, Topology }
import scala.util.Try
import java.util.{ ArrayList => JAList, List => JList }
import org.apache.storm.tuple.Fields
import StormTopologyBuilder._

private[storm] object Edges {
  def shuffleItemToItem[T](
    sourceId: Topology.EmittingId[Item[T]],
    destId: Topology.ReceivingId[Item[T]],
    withLocal: Boolean
  ): Topology.Edge[Item[T], Item[T]] =
    Topology.Edge[Item[T], Item[T]](
      sourceId,
      EdgeFormats.item[T],
      EdgeGrouping.shuffle(withLocal),
      identity,
      destId
    )

  def shuffleKeyValueToItem[K, V](
    sourceId: Topology.EmittingId[KeyValue[K, V]],
    destId: Topology.ReceivingId[Item[(K, V)]],
    withLocal: Boolean
  ): Topology.Edge[KeyValue[K, V], Item[(K, V)]] =
    Topology.Edge(
      sourceId,
      EdgeFormats.keyValue[K, V],
      EdgeGrouping.shuffle(withLocal),
      value => (value._1, (value._2, value._3)),
      destId
    )

  def groupedAggregatedToSummer[K, V](
    sourceId: Topology.EmittingId[Aggregated[K, V]],
    destId: Topology.ReceivingId[SummerInput[K, V]]
  ): Topology.Edge[Aggregated[K, V], SummerInput[K, V]] =
    Topology.Edge(
      sourceId,
      EdgeFormats.aggregated[K, V],
      EdgeGrouping.Fields(new Fields(EdgeFormats.shardKey)),
      _._2,
      destId
    )

  def shuffleShardedToItem[K, V](
    sourceId: Topology.EmittingId[Sharded[K, V]],
    destId: Topology.ReceivingId[Item[(K, V)]],
    withLocal: Boolean
  ): Topology.Edge[Sharded[K, V], Item[(K, V)]] =
    Topology.Edge(
      sourceId,
      EdgeFormats.sharded[K, V],
      EdgeGrouping.shuffle(withLocal),
      sharded => (sharded._3._1, (sharded._2._1, sharded._3._2)),
      destId
    )

  def groupedShardedToSummer[K, V](
    sourceId: Topology.EmittingId[Sharded[K, V]],
    destId: Topology.ReceivingId[SummerInput[K, V]]
  ): Topology.Edge[Sharded[K, V], SummerInput[K, V]] =
    Topology.Edge(
      sourceId,
      EdgeFormats.sharded[K, V],
      EdgeGrouping.Fields(new Fields(EdgeFormats.shardKey)),
      sharded => Some((sharded._2, sharded._3)),
      destId
    )
}

private object EdgeFormats {
  val shardKey = "shard"

  def item[T]: OutputFormat[Item[T]] =
    OutputFormat(List("timestamp", "value"), EdgeInjections.Pair())
  def aggregated[K, V]: OutputFormat[Aggregated[K, V]] =
    OutputFormat(List(shardKey, "aggregated"), EdgeInjections.Pair())
  def keyValue[K, V]: OutputFormat[KeyValue[K, V]] =
    OutputFormat(List("timestamp", "key", "value"), EdgeInjections.Triple())
  def sharded[K, V]: OutputFormat[Sharded[K, V]] =
    OutputFormat(List(shardKey, "keyWithBatch", "valueWithTimestamp"), EdgeInjections.Triple())
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
