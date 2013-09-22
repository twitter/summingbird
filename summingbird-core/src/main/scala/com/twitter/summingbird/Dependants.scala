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
case class Dependants[P <: Platform[P]](tail: Producer[P, _]) {
  lazy val allTails: List[Producer[P, _]] = nodes.filter { fanOut(_).get == 0 }
  val nodes: List[Producer[P, _]] = Producer.entireGraphOf(tail)

  /** This is the dependants graph. Each Producer knows who it depends on
   * but not who depends on it without doing this computation
   */
  private val graph: NeighborFn[Producer[P, _]] = {
    // The casts can be removed when Producer is covariant:
    val nfn = (Producer.dependenciesOf _).asInstanceOf[NeighborFn[Producer[P, Any]]]
    reversed(nodes.asInstanceOf[List[Producer[P, Any]]])(nfn)
      .asInstanceOf[NeighborFn[Producer[P, _]]]
  }
  private val depths: Map[Producer[P, _], Int] = {
    // The casts can be removed when Producer is covariant:
    val nfn = (Producer.dependenciesOf _).asInstanceOf[NeighborFn[Producer[P, Any]]]
    dagDepth(nodes.asInstanceOf[List[Producer[P, Any]]])(nfn)
      .asInstanceOf[Map[Producer[P, _], Int]]
  }
  /** The max of zero and 1 + depth of all parents if the node is the graph
   */
  def isNode(p: Producer[P, _]): Boolean = depth(p).isDefined
  def depth(p: Producer[P, _]): Option[Int] = depths.get(p)

  def dependantsOf(p: Producer[P, _]): Option[List[Producer[P, _]]] =
    if(isNode(p)) Some(graph(p).toList) else None

  def fanOut(p: Producer[P, _]): Option[Int] = dependantsOf(p).map { _.size }
  /**
   * Return all dependendants of a given node.
   * Does not include itself
   */
  def transitiveDependantsOf(p: Producer[P, _]): List[Producer[P, _]] = {
    // The casts can be removed when Producer is covariant:
    val nfn = graph.asInstanceOf[NeighborFn[Producer[P, Any]]]
    depthFirstOf(p.asInstanceOf[Producer[P, Any]])(nfn).toList.asInstanceOf[List[Producer[P, _]]]
  }
}
