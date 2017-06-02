package com.twitter.summingbird.storm

import com.twitter.bijection.{ Injection, Inversion }
import org.apache.storm.tuple.Fields
import java.util.{ ArrayList => JAList, List => JList }

import com.twitter.summingbird.online.executor.KeyValueShards
import org.apache.storm.topology.BoltDeclarer

import scala.collection.{ Map => CMap }
import scala.util.Try

sealed trait Edge[T] {
  val fields: Fields
  val injection: Injection[T, JList[AnyRef]]
  val grouping: EdgeGrouping
}

sealed trait EdgeGrouping {
  def apply(declarer: BoltDeclarer, parentName: String): Unit
}
case object ShuffleEdgeGrouping extends EdgeGrouping {
  override def apply(declarer: BoltDeclarer, parentName: String): Unit =
    declarer.shuffleGrouping(parentName)
}
case object LocalOrShuffleEdgeGrouping extends EdgeGrouping {
  override def apply(declarer: BoltDeclarer, parentName: String): Unit =
    declarer.localOrShuffleGrouping(parentName)
}
case class FieldsEdgeGrouping(fields: Fields) extends EdgeGrouping {
  override def apply(declarer: BoltDeclarer, parentName: String): Unit =
    declarer.fieldsGrouping(parentName, fields)
}

case class ItemEdge[T] private (edgeGrouping: EdgeGrouping) extends Edge[T] {
  override val fields: Fields = new Fields("value")
  override val injection: Injection[T, JList[AnyRef]] =  EdgeInjections.forItem()
  override val grouping: EdgeGrouping = edgeGrouping
}

object ItemEdge {
  def withShuffleGrouping[T](): ItemEdge[T] = ItemEdge[T](ShuffleEdgeGrouping)
  def withLocalOrShuffleGrouping[T](): ItemEdge[T] = ItemEdge[T](LocalOrShuffleEdgeGrouping)
}

case class AggregatedKeyValuesEdge[K, V](shards: KeyValueShards) extends Edge[(Int, CMap[K, V])] {
  override val fields: Fields = new Fields("aggKey", "aggValue")
  override val injection: Injection[(Int, CMap[K, V]), JList[AnyRef]] = EdgeInjections.forKeyValue()
  override val grouping: EdgeGrouping = FieldsEdgeGrouping(new Fields("aggKey"))
}

object EdgeInjections {
  def forItem[T](): Injection[T, JList[AnyRef]] = new Injection[T, JList[AnyRef]] {
    override def apply(t: T): JAList[AnyRef] = {
      val list = new JAList[AnyRef](1)
      list.add(t.asInstanceOf[AnyRef])
      list
    }

    override def invert(vin: JList[AnyRef]): Try[T] = Inversion.attempt(vin) { v =>
      v.get(0).asInstanceOf[T]
    }
  }

  def forKeyValue[K, V](): Injection[(K, V), JList[AnyRef]] = new Injection[(K, V), JList[AnyRef]] {
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
