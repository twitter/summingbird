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

package com.twitter.summingbird.online.executor

import scala.util.Try

trait OperationContainer[Input, Output, State] {
  def init(): Unit = {}
  def cleanup(): Unit = {}

  def executeTick: TraversableOnce[(Stream[State], Try[TraversableOnce[Output]])]
  def execute(state: State,
    data: Input): TraversableOnce[(Stream[State], Try[TraversableOnce[Output]])]
  def notifyFailure(inputs: Stream[State], e: Throwable): Unit = {}
}