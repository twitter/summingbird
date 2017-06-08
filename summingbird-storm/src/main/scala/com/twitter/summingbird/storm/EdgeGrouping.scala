package com.twitter.summingbird.storm

import org.apache.storm.topology.BoltDeclarer
import org.apache.storm.tuple.{ Fields => StormFields }

/**
  * This trait is used to represent different grouping strategies in `Storm`.
  */
sealed trait EdgeGrouping {
  /**
   * How to apply this `EdgeGrouping` to edge between `parentName` node and bolt declared by `declarer`.
   */
  def apply(declarer: BoltDeclarer, parentName: String): Unit
}

object EdgeGrouping {
  case object Shuffle extends EdgeGrouping {
    override def apply(declarer: BoltDeclarer, parentName: String): Unit =
      declarer.shuffleGrouping(parentName)
  }
  case object LocalOrShuffle extends EdgeGrouping {
    override def apply(declarer: BoltDeclarer, parentName: String): Unit =
      declarer.localOrShuffleGrouping(parentName)
  }
  case class Fields(fields: StormFields) extends EdgeGrouping {
    override def apply(declarer: BoltDeclarer, parentName: String): Unit =
      declarer.fieldsGrouping(parentName, fields)
  }
}
