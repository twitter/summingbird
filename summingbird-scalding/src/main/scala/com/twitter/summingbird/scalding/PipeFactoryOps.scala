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

import com.twitter.summingbird.batch.Timestamp
import com.twitter.scalding.TypedPipe

class PipeFactoryOps[+T](pipeFactory: PipeFactory[T]) {

  def flatMapElements[U](fn: (T => TraversableOnce[U])): PipeFactory[U] =
    mapPipe(_.flatMap {
      case (time, tup) =>
        fn(tup).map((time, _))
    })

  def mapElements[U](fn: (T => U)): PipeFactory[U] =
    flatMapElements({ tup => List(fn(tup)) })

  def mapPipe[U](fn: (TypedPipe[(Timestamp, T)] => TypedPipe[(Timestamp, U)])): PipeFactory[U] = {
    pipeFactory.map { flowProducer =>
      flowProducer.map(fn(_))
    }
  }
}
