package com.twitter.summingbird.storm

import com.twitter.bijection.{ Injection, Inversion }
import org.apache.storm.tuple.Fields
import java.util.{ ArrayList => JAList, List => JList }

import com.twitter.summingbird.online.executor.KeyValueShards

import scala.collection.{ Map => CMap }
import scala.util.Try

/**
  * This trait represents an edge in Storm topology DAG between two nodes.
  * @tparam T represents type of tuples sent over this `Edge`.
  */
sealed trait Edge[T] {
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

object Edge {
  /**
    * Simplest possible type of `Edge`, without any assumptions about content inside.
    */
  case class Item[T] private[storm] (edgeGrouping: EdgeGrouping) extends Edge[T] {
    override val fields: Fields = new Fields("value")
    override val injection: Injection[T, JList[AnyRef]] =  EdgeInjections.forItem
    override val grouping: EdgeGrouping = edgeGrouping
  }

  /**
    * This `Edge` type used for aggregated key value pairs emitted by partial aggregation.
    * @param shards is a number which was used for partial aggregation.
    */
  case class AggregatedKeyValues[K, V](shards: KeyValueShards) extends Edge[(Int, CMap[K, V])] {
    override val fields: Fields = new Fields("aggKey", "aggValue")
    override val injection: Injection[(Int, CMap[K, V]), JList[AnyRef]] = EdgeInjections.forKeyValue
    override val grouping: EdgeGrouping = EdgeGrouping.Fields(new Fields("aggKey"))
  }

  def itemWithShuffleGrouping[T]: Item[T] = Item[T](EdgeGrouping.Shuffle)
  def itemWithLocalOrShuffleGrouping[T]: Item[T] = Item[T](EdgeGrouping.LocalOrShuffle)
}

private object EdgeInjections {
  def forItem[T]: Injection[T, JList[AnyRef]] = new Injection[T, JList[AnyRef]] {
    override def apply(tuple: T): JAList[AnyRef] = {
      val list = new JAList[AnyRef](1)
      list.add(tuple.asInstanceOf[AnyRef])
      list
    }

    override def invert(valueIn: JList[AnyRef]): Try[T] = Inversion.attempt(valueIn) { input =>
      input.get(0).asInstanceOf[T]
    }
  }

  def forKeyValue[K, V]: Injection[(K, V), JList[AnyRef]] = new Injection[(K, V), JList[AnyRef]] {
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
