package com.twitter.summingbird.storm

import backtype.storm.spout.{ ISpoutOutputCollector, SpoutOutputCollector }
import scala.collection.mutable.{ Set => MSet }
import java.util
import org.scalacheck._

/**
 * Created by pnaramsetti on 8/19/16.
 */
class TestAggregateOutpoutCollector(in: ISpoutOutputCollector, expected: MSet[(Int, Map[_, _])]) extends SpoutOutputCollector(in) {

  private val expectedTuples = expected

  def checkSize: Int = {
    expectedTuples.size
  }

  override def emit(
    s: String,
    list: util.List[AnyRef],
    o: scala.Any): util.List[Integer] = {
    emit(list)
  }

  override def emit(
    list: util.List[AnyRef],
    o: scala.Any): util.List[Integer] = {
    emit(list)
  }

  override def emit(list: util.List[AnyRef]): util.List[Integer] = {
    val key = list.get(0).asInstanceOf[Int]
    val value = list.get(1).asInstanceOf[Map[_, _]]
    if (expectedTuples.contains((key, value))) {
      expectedTuples.remove((key, value))
    } else {
      println(("Expected tuple (%s, %s) is not expected to be emitted".format(key, value)))
      assert(false)
    }
    null
  }

  override def emit(
    s: String,
    list: util.List[AnyRef]): util.List[Integer] = {
    emit(list)
  }
}
