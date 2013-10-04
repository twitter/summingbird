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

import com.twitter.algebird.Interval

/**
 * Job state models the memory of when the next job should
 * try to cover
 */
trait WaitingState[T] {
  // Record that you are running, and get the starting time:
  def begin: RunningState[T]
}

trait RunningState[T] {
  def part: Interval[T]
  /** suceedPart MAY not be equal to part, it might be a subset
   */
  def succeed(succeedPart: Interval[T]): WaitingState[T]
  def fail(err: Throwable): WaitingState[T]
}
