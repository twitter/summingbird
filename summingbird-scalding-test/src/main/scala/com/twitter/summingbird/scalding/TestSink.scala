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

import com.twitter.algebird.monad._
import com.twitter.summingbird.batch._

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }
import com.twitter.scalding.typed.TypedSink

/**
 * This is a test sink that assumes single threaded testing with
 * cascading local mode
 */
class TestSink[T] extends Sink[T] {
  private var data: Vector[(Timestamp, T)] = Vector.empty

  def write(incoming: PipeFactory[T]): PipeFactory[T] =
    // three functors deep:
    incoming.map { state =>
      state.map { reader =>
        reader.map { timeItem =>
          data = data :+ timeItem
          timeItem
        }
      }
    }

  def reset: Vector[(Timestamp, T)] = {
    val oldData = data
    data = Vector.empty
    oldData
  }
}
