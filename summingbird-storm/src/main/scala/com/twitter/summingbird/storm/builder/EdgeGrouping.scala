package com.twitter.summingbird.storm.builder

import org.apache.storm.topology.BoltDeclarer
import org.apache.storm.tuple.{ Fields => StormFields }

/**
  * This trait is used to represent different grouping strategies in `Storm`.
  */
sealed trait EdgeGrouping {
  /**
   * How to apply this `EdgeGrouping` to edge between `parentName` node and bolt declared by `declarer`.
   */
  def apply(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit
}

object EdgeGrouping {
  case object Shuffle extends EdgeGrouping {
    override def apply(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit =
      declarer.shuffleGrouping(parent.id)
  }
  case object LocalOrShuffle extends EdgeGrouping {
    override def apply(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit =
      declarer.localOrShuffleGrouping(parent.id)
  }
  case class Fields(fields: StormFields) extends EdgeGrouping {
    override def apply(declarer: BoltDeclarer, parent: Topology.EmittingId[_]): Unit =
      declarer.fieldsGrouping(parent.id, fields)
  }
}
