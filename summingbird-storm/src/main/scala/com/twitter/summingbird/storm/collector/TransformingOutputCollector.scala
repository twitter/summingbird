package com.twitter.summingbird.storm.collector

import backtype.storm.spout.SpoutOutputCollector
import java.util.{ List => JList }

/**
 * The TransformingOutputCollector is used to transform the Value object when passed on by the Spout.
 */

class TransformingOutputCollector(in: SpoutOutputCollector, func: JList[AnyRef] => JList[AnyRef]) extends SpoutOutputCollector(in) {

  override def emitDirect(i: Int, s: String, list: JList[AnyRef], o: scala.AnyRef): Unit = in.emitDirect(i, s, func(list), o)

  override def emitDirect(i: Int, list: JList[AnyRef], o: scala.AnyRef): Unit = in.emitDirect(i, func(list), o)

  override def emitDirect(i: Int, s: String, list: JList[AnyRef]): Unit = in.emitDirect(i, s, func(list))

  override def emitDirect(i: Int, list: JList[AnyRef]): Unit = in.emitDirect(i, func(list))

  override def emit(s: String, list: JList[AnyRef], o: scala.AnyRef): JList[Integer] = in.emit(s, func(list), o)

  override def emit(list: JList[AnyRef], o: scala.AnyRef): JList[Integer] = in.emit(func(list), o)

  override def emit(list: JList[AnyRef]): JList[Integer] = in.emit(func(list))

  override def emit(s: String, list: JList[AnyRef]): JList[Integer] = in.emit(s, func(list))

  override def reportError(throwable: Throwable): Unit = in.reportError(throwable)
}
