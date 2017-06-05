package com.twitter.summingbird.storm

import com.twitter.bijection.{ Injection, Inversion }
import org.apache.storm.tuple.Fields
import java.util.{ ArrayList => JAList, List => JList }

import com.twitter.summingbird.online.executor.KeyValueShards

import scala.collection.{ Map => CMap }
import scala.util.Try

sealed trait Edge[T] {
  val fields: Fields
  val injection: Injection[T, JList[AnyRef]]
  val grouping: EdgeGrouping
}

object Edge {
  case class Item[T] private[storm] (edgeGrouping: EdgeGrouping) extends Edge[T] {
    override val fields: Fields = new Fields("value")
    override val injection: Injection[T, JList[AnyRef]] =  EdgeInjections.forItem
    override val grouping: EdgeGrouping = edgeGrouping
  }

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
    override def apply(t: T): JAList[AnyRef] = {
      val list = new JAList[AnyRef](1)
      list.add(t.asInstanceOf[AnyRef])
      list
    }

    override def invert(vin: JList[AnyRef]): Try[T] = Inversion.attempt(vin) { v =>
      v.get(0).asInstanceOf[T]
    }
  }

  def forKeyValue[K, V]: Injection[(K, V), JList[AnyRef]] = new Injection[(K, V), JList[AnyRef]] {
    override def apply(item: (K, V)): JAList[AnyRef] = {
      val (key, v) = item
      val list = new JAList[AnyRef](2)
      list.add(key.asInstanceOf[AnyRef])
      list.add(v.asInstanceOf[AnyRef])
      list
    }

    override def invert(vin: JList[AnyRef]): Try[(K, V)] = Inversion.attempt(vin) { v =>
      val key = v.get(0).asInstanceOf[K]
      val value = v.get(1).asInstanceOf[V]
      (key, value)
    }
  }
}
