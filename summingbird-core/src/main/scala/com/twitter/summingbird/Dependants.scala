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

/** Producers are Directed Acyclic Graphs
 * by the fact that they are immutable.
 */
case class Dependants[P <: Platform[P]](tail: Producer[P, Any]) {
  lazy val nodes: List[Producer[P, Any]] = Producer.entireGraphOf(tail)
  lazy val allTails: List[Producer[P, Any]] = nodes.filter { fanOut(_).get == 0 }
  private lazy val nodeSet: Set[Producer[P, Any]] = nodes.toSet

  /** This is the dependants graph. Each Producer knows who it depends on
   * but not who depends on it without doing this computation
   */
  private val graph: NeighborFn[Producer[P, Any]] = {
    val nfn = Producer.dependenciesOf[P](_)
    reversed(nodes)(nfn)
  }
  private val depths: Map[Producer[P, Any], Int] = {
    val nfn = Producer.dependenciesOf[P](_)
    dagDepth(nodes)(nfn)
  }
  /** The max of zero and 1 + depth of all parents if the node is the graph
   */
  def isNode(p: Producer[P, Any]): Boolean = nodeSet.contains(p)
  def depth(p: Producer[P, Any]): Option[Int] = depths.get(p)

  def dependantsOf(p: Producer[P, Any]): Option[List[Producer[P, Any]]] =
    if(isNode(p)) Some(graph(p).toList) else None

  def fanOut(p: Producer[P, Any]): Option[Int] = dependantsOf(p).map { _.size }
  /**
   * Return all dependendants of a given node.
   * Does not include itself
   */
  def transitiveDependantsOf(p: Producer[P, Any]): List[Producer[P, Any]] =
    depthFirstOf(p)(graph).toList

  /** Return all the dependants of this node, but don't go past any output
   * nodes, such as Summer or WrittenProducer. This is for searching downstream
   * until write/sum to see if certain optimizations can be enabled
   */
  def transitiveDependantsTillOutput(p: Producer[P, Any]): List[Producer[P, Any]] = {
    val neighborFn = { (p: Producer[P, Any]) =>
      if(Producer.isOutput(p)) Iterable.empty else graph(p)
    }
    depthFirstOf(p)(neighborFn).toList
  }
}
