/*
 Copyright 2015 Twitter, Inc.

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

package com.twitter.summingbird.planner

/**
 * This is a marker trait that indicates that
 * the subclass holds one or more irreducible
 * items passed by the user. These may need to
 * be accessed at some planning stage after
 * optimization in order to attach the correct
 * options
 */
trait FunctionContainer {
  def innerFunctions: Iterable[Function1[_, _]]
}

object FunctionContainer {
  def flatten(item: Function1[_, _]): Iterable[Function1[_, _]] = item match {
    case (ic: FunctionContainer) => ic.innerFunctions
    case _ => Seq(item)
  }

  def flatten(left: Function1[_, _], right: Function1[_, _]): Iterable[Function1[_, _]] = (left, right) match {
    case (li: FunctionContainer, ri: FunctionContainer) => li.innerFunctions ++ ri.innerFunctions
    case (li: FunctionContainer, _) => li.innerFunctions ++ Seq(right)
    case (_, ri: FunctionContainer) => Seq(left) ++ ri.innerFunctions
    case _ => Seq(left, right)
  }
}
