package com.twitter.summingbird.storm.builder

import org.apache.storm.topology.BoltDeclarer
import scala.collection.JavaConverters.bufferAsJavaListConverter
import org.apache.storm.tuple.{ Fields => StormFields }
import scala.collection.mutable.ListBuffer

/**
  * This trait is used to represent different grouping strategies in Storm.
  */
private[summingbird] sealed trait EdgeGrouping {
  /**
   * How to register this EdgeGrouping to edge between `parentName` node and bolt declared by `declarer`.
   */
  def register(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit
}

private[summingbird] object EdgeGrouping {
  case object Shuffle extends EdgeGrouping {
    override def register(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit =
      declarer.shuffleGrouping(parent.id)
  }
  case object LocalOrShuffle extends EdgeGrouping {
    override def register(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit =
      declarer.localOrShuffleGrouping(parent.id)
  }
  case class Fields(fields: List[String]) extends EdgeGrouping {
    override def register(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit =
      declarer.fieldsGrouping(parent.id, new StormFields(ListBuffer(fields: _*).asJava))
  }
}
