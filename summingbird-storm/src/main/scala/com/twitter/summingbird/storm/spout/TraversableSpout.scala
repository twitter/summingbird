/*
 Copyright 2012 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.summingbird.storm.spout

import org.apache.storm.tuple.Values
import com.twitter.tormenta.spout.Spout
import java.util.ArrayList
import collection.JavaConverters._

object TraversableSpout {
  def apply[T](items: TraversableOnce[T], fieldName: String = "item"): TraversableSpout[T] =
    new TraversableSpout(items, fieldName)
}

class TraversableSpout[+T](items: TraversableOnce[T], fieldName: String) extends Spout[T] {
  private def wrap[T](t: T) = new Values(t.asInstanceOf[AnyRef])

  lazy val tupleList = items.toList
  lazy val javaList = new ArrayList(tupleList.map(wrap).asJava)

  override def getSpout = new FixedTupleSpout(javaList, fieldName)

  def flatMap[U](fn: T => TraversableOnce[U]) =
    new TraversableSpout(tupleList.flatMap(fn), fieldName)
}
