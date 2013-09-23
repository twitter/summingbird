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

/** Collection of graph algorithms */
package object graph {
  type NeighborFn[T] = (T => Iterable[T])

  /** Return the depth first enumeration of reachable nodes,
   * NOT INCLUDING INPUT, unless it can be reached via neighbors */
  def depthFirstOf[T](t: T)(nf: NeighborFn[T]): IndexedSeq[T] = {
    @annotation.tailrec
    def loop(stack: List[T], deps: Vector[T], acc: Set[T]): (Vector[T], Set[T]) = {
      stack match {
        case Nil => (deps, acc)
        case h::tail =>
          val newStack = nf(h).filterNot(acc).foldLeft(tail) { (s, it) => it :: s }
          val newDeps = if(acc(h)) deps else (deps :+ h)
          loop(newStack, newDeps, acc + h)
      }
    }
    val start = nf(t)
    loop(start.toList, Vector(start.toSeq.distinct : _*), start.toSet)._1
  }

  /** Return a NeighborFn for the graph of reversed edges defined by
   * this set of nodes and nf
   * We avoid Sets which use hash-codes which may depend on addresses
   * which are not stable from one run to the next.
   */
  def reversed[T](nodes: Iterable[T])(nf: NeighborFn[T]): NeighborFn[T] = {
    val graph: Map[T, List[T]] = nodes
        .foldLeft(Map.empty[T,List[T]]) { (g, child) =>
          val gWithChild = g + (child -> g.getOrElse(child, Nil))
          nf(child).foldLeft(gWithChild) { (innerg, parent) =>
            innerg + (parent -> (child :: innerg.getOrElse(parent, Nil)))
          }
        }
        // make sure the values are sets, not .mapValues is lazy in scala
        .map { case (k,v) => (k, v.distinct) };
    graph.getOrElse(_, Nil)
  }

  /** Return the depth of each node in the dag.
   * a node that has no dependencies has depth == 0
   * else it is max of parent + 1
   *
   * Behavior is not defined if the graph is not a DAG (for now, it runs forever, may throw later)
   */
  def dagDepth[T](nodes: Iterable[T])(nf: NeighborFn[T]): Map[T, Int] = {
    @annotation.tailrec
    def computeDepth(todo: Set[T], acc: Map[T, Int]): Map[T, Int] =
      if(todo.isEmpty) acc
      else {
        def withParents(n: T) = (n :: (nf(n).toList)).filterNot(acc.contains(_)).distinct

        val (doneThisStep, rest) = todo.map { withParents(_) }.partition { _.size == 1 }
        val newAcc = acc ++ (doneThisStep.flatten.map { n =>
          val depth = nf(n) //n is done now, so all it's neighbors must be too.
            .map { acc(_) + 1 }
            .reduceOption { _ max _ }
            .getOrElse(0)
          n -> depth
        })
        computeDepth(rest.flatten, newAcc)
      }
    computeDepth(nodes.toSet, Map.empty)
  }
}
