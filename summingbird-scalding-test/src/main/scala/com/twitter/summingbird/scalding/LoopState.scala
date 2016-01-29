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

// This is not really usable, just a mock that does the same state over and over
class LoopState[T](init: T) extends WaitingState[T] { self =>
  var failed: Boolean = false
  def begin = new PrepareState[T] {
    def requested = self.init
    def fail(err: Throwable) = {
      println(err)
      failed = true
      self
    }
    def willAccept(intr: T) = Right(new RunningState[T] {
      def succeed = self
      def fail(err: Throwable) = {
        println(err)
        failed = true
        self
      }
    })
  }
}
