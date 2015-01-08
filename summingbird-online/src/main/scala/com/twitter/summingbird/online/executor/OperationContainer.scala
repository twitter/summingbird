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
import com.twitter.bijection.Injection

trait OperationContainer[Input, Output, State, WireFmt, RuntimeContext] {
  def decoder: Injection[Input, WireFmt]
  def encoder: Injection[Output, WireFmt]
  def executeTick: TraversableOnce[(Seq[State], Try[TraversableOnce[Output]])]
  def execute(state: State,
    data: Input): TraversableOnce[(Seq[State], Try[TraversableOnce[Output]])]
  def init(ctx: RuntimeContext) {}
  def cleanup {}
  def notifyFailure(inputs: Seq[State], e: Throwable) {}
}