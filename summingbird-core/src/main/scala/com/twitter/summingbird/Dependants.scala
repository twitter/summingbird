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

/** Producers are Directed Acyclic Graphs
 * by the fact that they are immutable.
 */
case class Dependants[P <: Platform[P]](tail: Producer[P, _]) {
  private val graph: Map[Producer[P, _], Set[Producer[P, _]]] = {
    val empty = Map[Producer[P, _], Set[Producer[P, _]]]()

    (Producer.transitiveDependenciesOf(tail) + tail)
      .foldLeft(empty) { (graph, child) =>
        val withChild = graph + (child -> graph.getOrElse(child, Set[Producer[P, _]]()))
        Producer.dependenciesOf(child)
          .foldLeft(withChild) { (innerg, parent) =>
            innerg + (parent -> (innerg.getOrElse(parent, Set[Producer[P, _]]()) + child))
          }
      }
  }
  private val depths: Map[Producer[P, _], Int] = computeDepth(graph.keys.toSet, Map.empty)

  @annotation.tailrec
  private def computeDepth(todo: Set[Producer[P, _]], acc: Map[Producer[P, _], Int]): Map[Producer[P, _], Int] =
    if(todo.isEmpty) acc
    else {
      def withParents(n: Producer[P, _]) = (Producer.dependenciesOf(n) + n).filterNot { acc.contains(_) }

      val (done, rest) = todo.map { withParents(_) }.partition { _.size == 1 }
      val newTodo = rest.flatten
      val newAcc = acc ++ (done.flatten.map { n =>
        val depth = Producer.dependenciesOf(n)
          .map { acc(_) + 1 }
          .reduceOption { _ max _ }
          .getOrElse(0)
        n -> depth
      })
      computeDepth(newTodo, newAcc)
    }
  /** The max of zero and 1 + depth of all parents if the node is the graph
   */
  def depth(p: Producer[P, _]): Option[Int] = depths.get(p)
  def dependantsOf(p: Producer[P, _]): Option[Set[Producer[P, _]]] = graph.get(p)

  def fanOut(p: Producer[P, _]): Option[Int] = dependantsOf(p).map { _.size }
}
