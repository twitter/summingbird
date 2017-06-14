package com.twitter.summingbird.storm.builder

import com.twitter.bijection.{ Injection, Inversion }
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import java.util.{ ArrayList => JAList, List => JList }
import org.apache.storm.tuple.Fields
import scala.collection.{ Map => CMap }
import scala.util.Try

/**
  * This trait represents an edge in Storm topology DAG between two nodes.
  * @tparam T represents type of tuples sent over this `Edge`.
  */
private[summingbird] sealed trait EdgeType[T] {
  /**
    * Storm's `Fields` are going to be sent over this `Edge`.
    */
  val fields: Fields

  /**
    * Injection from values (which are going to be sent) to Storm's values.
    * Each element in returned array corresponds to element in `Fields` at the same index.
    */
  val injection: Injection[T, JList[AnyRef]]

  /**
    * Grouping for this `Edge`.
    */
  val grouping: EdgeGrouping
}

private[summingbird] object EdgeType {
  private val shardKey = "shard"

  /**
    * Simplest possible type of `Edge`, without any assumptions about content inside.
    */
  case class Item[T] private[storm] (edgeGrouping: EdgeGrouping) extends EdgeType[(Timestamp, T)] {
    override val fields: Fields = new Fields("timestamp", "value")
    override val injection: Injection[(Timestamp, T), JList[AnyRef]] =
      EdgeTypeInjections.Pair()
    override val grouping: EdgeGrouping = edgeGrouping
  }

  /**
    * This `Edge` type used for aggregated key value pairs emitted by partial aggregation.
    */
  case class AggregatedKeyValues[K, V]() extends EdgeType[(Int, CMap[(K, BatchID), (Timestamp, V)])] {
    override val fields: Fields = new Fields(shardKey, "aggregated")
    override val injection: Injection[(Int, CMap[(K, BatchID), (Timestamp, V)]), JList[AnyRef]] =
      EdgeTypeInjections.Pair()
    override val grouping: EdgeGrouping = EdgeGrouping.Fields(new Fields(shardKey))
  }

  case class KeyValue[K, V] private[storm](edgeGrouping: EdgeGrouping) extends EdgeType[(Timestamp, K, V)] {
    override val fields: Fields = new Fields("timestamp", "key", "value")
    override val injection: Injection[(Timestamp, K, V), JList[AnyRef]] =
      EdgeTypeInjections.Triple()
    override val grouping: EdgeGrouping = edgeGrouping
  }

  case class ShardedKeyValue[K, V] private[storm](
    edgeGrouping: EdgeGrouping
  ) extends EdgeType[(Int, (K, BatchID), (Timestamp, V))] {
    override val fields: Fields = new Fields(shardKey, "keyWithBatch", "valueWithTimestamp")
    override val injection: Injection[(Int, (K, BatchID), (Timestamp, V)), JList[AnyRef]] =
      EdgeTypeInjections.Triple()
    override val grouping: EdgeGrouping = edgeGrouping
  }

  def item[T](withLocal: Boolean): Item[T] = Item(EdgeGrouping.shuffle(withLocal))
  def keyValue[K, V](withLocal: Boolean): KeyValue[K, V] = KeyValue(EdgeGrouping.shuffle(withLocal))
  def groupedShardedKeyValue[K, V]: ShardedKeyValue[K, V] =
    ShardedKeyValue(EdgeGrouping.Fields(new Fields(shardKey)))
  def shuffledShardedKeyValue[K, V](withLocal: Boolean): ShardedKeyValue[K, V] =
    ShardedKeyValue(EdgeGrouping.shuffle(withLocal))
}

private object EdgeTypeInjections {
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
