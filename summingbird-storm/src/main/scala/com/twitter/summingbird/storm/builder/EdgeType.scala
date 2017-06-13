package com.twitter.summingbird.storm.builder

import com.twitter.bijection.{ Injection, Inversion }
import com.twitter.summingbird.online.executor.KeyValueShards
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
  /**
    * Simplest possible type of `Edge`, without any assumptions about content inside.
    */
  case class Item[T] private[storm] (edgeGrouping: EdgeGrouping) extends EdgeType[T] {
    override val fields: Fields = new Fields("value")
    override val injection: Injection[T, JList[AnyRef]] = EdgeTypeInjections.Item()
    override val grouping: EdgeGrouping = edgeGrouping
  }

  /**
    * This `Edge` type used for aggregated key value pairs emitted by partial aggregation.
    * @param shards is a number which was used for partial aggregation.
    */
  case class AggregatedKeyValues[K, V](shards: KeyValueShards) extends EdgeType[(Int, CMap[K, V])] {
    override val fields: Fields = new Fields("aggKey", "aggValue")
    override val injection: Injection[(Int, CMap[K, V]), JList[AnyRef]] = EdgeTypeInjections.KeyValue()
    override val grouping: EdgeGrouping = EdgeGrouping.Fields(new Fields("aggKey"))
  }

  def item[T](withLocal: Boolean): Item[T] = Item(EdgeGrouping.shuffle(withLocal))
}

private object EdgeTypeInjections {
  case class Item[T]() extends Injection[T, JList[AnyRef]] {
    override def apply(tuple: T): JAList[AnyRef] = {
      val list = new JAList[AnyRef](1)
      list.add(tuple.asInstanceOf[AnyRef])
      list
    }

    override def invert(valueIn: JList[AnyRef]): Try[T] = Inversion.attempt(valueIn) { input =>
      input.get(0).asInstanceOf[T]
    }
  }

  case class KeyValue[K, V]() extends Injection[(K, V), JList[AnyRef]] {
    override def apply(tuple: (K, V)): JAList[AnyRef] = {
      val (key, value) = tuple
      val list = new JAList[AnyRef](2)
      list.add(key.asInstanceOf[AnyRef])
      list.add(value.asInstanceOf[AnyRef])
      list
    }

    override def invert(valueIn: JList[AnyRef]): Try[(K, V)] = Inversion.attempt(valueIn) { input =>
      val key = input.get(0).asInstanceOf[K]
      val value = input.get(1).asInstanceOf[V]
      (key, value)
    }
  }
}
