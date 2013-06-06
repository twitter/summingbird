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

import com.twitter.scalding.TypedPipe
import cascading.flow.FlowDef
import com.twitter.scalding.Mode
import com.twitter.summingbird.batch.BatchID
import java.io.Serializable


object BatchProducer extends Serializable {
  def const[T](t: => T) = new BatchProducer[T] { def apply(b: BatchID, fd: FlowDef, m: Mode) = t }
  implicit def fromFn[T](fn: (BatchID, FlowDef, Mode) => T): BatchProducer[T] =
    new BatchProducer[T] { def apply(b: BatchID, fd: FlowDef, m: Mode) = fn(b, fd, m) }
}

trait BatchProducer[+T] extends ((BatchID, FlowDef, Mode) => T) with Serializable { self =>
  def flatMap[U](bp: T => BatchProducer[U]): BatchProducer[U] =
    new BatchProducer[U] { def apply(b: BatchID, fd: FlowDef, m: Mode) = bp(self(b, fd, m)).apply(b, fd, m) }

  def map[U](fn: T => U): BatchProducer[U] =
    new BatchProducer[U] { def apply(b: BatchID, fd: FlowDef, m: Mode) = fn(self(b, fd, m)) }
}

