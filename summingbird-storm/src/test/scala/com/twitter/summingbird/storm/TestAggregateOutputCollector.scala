package com.twitter.summingbird.storm

import org.apache.storm.spout.{ ISpoutOutputCollector, SpoutOutputCollector }
import scala.collection.mutable.{ Set => MSet }
import java.util
import org.scalacheck._

object TestAggregateOutpoutCollector {
  type ExpectedTuple = (Int, Map[_, _], Option[String], Option[Seq[Any]])
  def emptyTupleSet = MSet.empty[ExpectedTuple]
}

class TestAggregateOutpoutCollector(in: ISpoutOutputCollector, expectedTuplesToBeSent: MSet[TestAggregateOutpoutCollector.ExpectedTuple]) extends SpoutOutputCollector(in) {

  private val expectedTuples = expectedTuplesToBeSent

  def getSize: Int = {
    expectedTuples.size
  }

  val ret = new util.ArrayList[Integer]

  def emitCheck(
    list: util.List[AnyRef],
    streamOpt: Option[String],
    messageIdOpt: Option[Any]): util.List[Integer] = {
    val key = list.get(0).asInstanceOf[Int]
    val value = list.get(1).asInstanceOf[Map[_, _]]
    val messageIdOptAsSeq = messageIdOpt.map {
      case iter: TraversableOnce[_] => iter.toSeq
    }
    if (expectedTuples.contains((key, value, streamOpt, messageIdOptAsSeq))) {
      expectedTuples.remove((key, value, streamOpt, messageIdOptAsSeq))
    } else {
      println(("Emitted tuple (%s, %s, %s, %s) is not expected to be emitted".format(key, value, streamOpt, messageIdOptAsSeq)))
      assert(false)
    }
    ret
  }

  override def emit(
    s: String,
    list: util.List[AnyRef],
    o: scala.Any): util.List[Integer] =
    emitCheck(list, Some(s), Some(o))

  override def emit(
    list: util.List[AnyRef],
    o: scala.Any): util.List[Integer] =
    emitCheck(list, None, Some(o))

  override def emit(list: util.List[AnyRef]): util.List[Integer] =
    emitCheck(list, None, None)

  override def emit(
    s: String,
    list: util.List[AnyRef]): util.List[Integer] =
    emitCheck(list, Some(s), None)
}
