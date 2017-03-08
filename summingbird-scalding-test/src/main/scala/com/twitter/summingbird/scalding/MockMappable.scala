/*
 Copyright 2013 Twitter, Inc.

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

package com.twitter.summingbird.scalding

import cascading.scheme.NullScheme
import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }
import java.io.{ InputStream, OutputStream }
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

class MockMappable[T](val id: String)(implicit tconv: TupleConverter[T])
    extends ScaldingSource with Mappable[T] {
  def converter[U >: T] = TupleConverter.asSuperConverter(tconv)
  override def toString = id
  override def equals(that: Any) = that match {
    case m: MockMappable[_] => m.id == id
    case _ => false
  }
  override def hashCode = id.hashCode

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    (readOrWrite, mode) match {
      case (Write, TestMode(buffers)) =>
        /*
         * We copy this code from scalding because scalding ALWAYS erases the target
         * whether or not a job is run. This is not what HDFS would do, and it violates
         * the assumption that planning is a pure function (running of course is not)
        */
        require(
          buffers(this).isDefined,
          TestTapFactory.sourceNotFoundError.format(this))

        new MemoryTap[InputStream, OutputStream](
          new NullScheme(Fields.ALL, Fields.ALL),
          buffers(this).get)

      case _ =>
        TestTapFactory(this, new NullScheme[JobConf, RecordReader[_, _], OutputCollector[_, _], T, T](Fields.ALL, Fields.ALL)).createTap(readOrWrite)
    }
}
