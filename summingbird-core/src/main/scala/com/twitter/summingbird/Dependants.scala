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

package com.twitter.summingbird

import com.twitter.summingbird.graph._

/**
 * Producers are Directed Acyclic Graphs
 * by the fact that they are immutable.
 */
case class Dependants[P <: Platform[P]](tail: Producer[P, Any]) extends DependantGraph[Producer[P, Any]] {
  override lazy val nodes: List[Producer[P, Any]] = Producer.entireGraphOf(tail)
  override def dependenciesOf(p: Producer[P, Any]) = Producer.dependenciesOf(p)

  /**
   * Return all the dependants of this node, but don't go past any output
   * nodes, such as Summer or WrittenProducer. This is for searching downstream
   * until write/sum to see if certain optimizations can be enabled
   */
  def transitiveDependantsTillOutput(inp: Producer[P, Any]): List[Producer[P, Any]] = {
    val neighborFn = { (p: Producer[P, Any]) =>
      p match {
        case t: TailProducer[_, _] => Iterable.empty // all legit writes are tails
        case _ => dependantsOf(p).getOrElse(Nil)
      }
    }
    depthFirstOf(inp)(neighborFn).toList
  }

  /**
   * Return the first dependants of this node AFTER merges. This will include
   * no MergedProducer nodes, but all parents of the returned nodes will be Merged
   * or the argument.
   */
  def dependantsAfterMerge(inp: Producer[P, Any]): List[Producer[P, Any]] =
    dependantsOf(inp).getOrElse(Nil).flatMap {
      case m @ MergedProducer(_, _) => dependantsAfterMerge(m)
      case AlsoProducer(_, r) => dependantsAfterMerge(r)
      case prod => List(prod)
    }

  def namesOf(inp: Producer[P, Any]): List[NamedProducer[P, Any]] =
    transitiveDependantsOf(inp).collect { case n @ NamedProducer(_, _) => n }
}
