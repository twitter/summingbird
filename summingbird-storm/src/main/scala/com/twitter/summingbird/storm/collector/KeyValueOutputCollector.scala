package com.twitter.summingbird.storm.collector

import backtype.storm.spout.SpoutOutputCollector
import java.util.{ List => JList }

/**
 * Created by pnaramsetti on 7/19/16.
 *
 * The KeyValueOutputCollector is used to unwrap the Value object when passed on by the Spout.
 */
class KeyValueOutputCollector(self: SpoutOutputCollector, func: JList[AnyRef] => JList[AnyRef]) extends SpoutOutputCollector(null) {

  override def emitDirect(i: Int, s: String, list: JList[AnyRef], o: scala.Any): Unit = self.emitDirect(i, s, func(list), o)

  override def emitDirect(i: Int, list: JList[AnyRef], o: scala.Any): Unit = self.emitDirect(i, func(list), o)

  override def emitDirect(i: Int, s: String, list: JList[AnyRef]): Unit = self.emitDirect(i, s, func(list))

  override def emitDirect(i: Int, list: JList[AnyRef]): Unit = self.emitDirect(i, func(list))

  override def emit(s: String, list: JList[AnyRef], o: scala.Any): JList[Integer] = self.emit(s, func(list), o)

  override def emit(list: JList[AnyRef], o: scala.Any): JList[Integer] = self.emit(func(list), o)

  override def emit(list: JList[AnyRef]): JList[Integer] = self.emit(func(list))

  override def emit(s: String, list: JList[AnyRef]): JList[Integer] = self.emit(s, func(list))

  override def reportError(throwable: Throwable): Unit = self.reportError(throwable)
}