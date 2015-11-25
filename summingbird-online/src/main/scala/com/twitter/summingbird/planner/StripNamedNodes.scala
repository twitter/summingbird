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

package com.twitter.summingbird.planner

import com.twitter.summingbird._
import scala.collection.breakOut

case class ProducerF[P <: Platform[P]](oldSources: List[Producer[P, Any]],
  oldRef: Producer[P, Any],
  f: List[Producer[P, Any]] => Producer[P, Any])

object StripNamedNode {

  private[this] def castTail[P <: Platform[P], T](node: Producer[P, T]): TailProducer[P, T] =
    node.asInstanceOf[TailProducer[P, T]]

  /**
   * Names apply to the sources, sinks, services, stores, semigroups and functions that the user passes in
   * This returns those
   */
  private def irreducible[P <: Platform[P]](node: Producer[P, Any]): Option[Any] =
    node match {
      case Source(src) => Some(src)
      case OptionMappedProducer(_, fn) => Some(fn)
      case FlatMappedProducer(_, fn) => Some(fn)
      case ValueFlatMappedProducer(_, fn) => Some(fn)
      case KeyFlatMappedProducer(_, fn) => Some(fn)
      case LeftJoinedProducer(_, serv) => Some(serv)
      case Summer(_, store, semi) => Some((store, semi))
      case WrittenProducer(_, sink) => Some(sink)
      // The following have nothing to put options on:
      case AlsoProducer(_, producer) => None
      case NamedProducer(producer, _) => None
      case IdentityKeyedProducer(producer) => None
      case MergedProducer(l, r) => None
      case _ => sys.error("Unreachable. Here to warn us if we add Producer subclasses but forget to update this")
    }

  def apply[P <: Platform[P], T](tail: TailProducer[P, T]): (Map[Producer[P, Any], List[String]], TailProducer[P, T]) = {
    val dagOpt = new DagOptimizer[P] {}
    // It must be a tail, but the optimizer doesn't retain that information
    val newTail = castTail(dagOpt.optimize(tail, dagOpt.RemoveNames))
    /**
     * Two nodes are the same if the irreducibles of their transitive dependencies are the same.
     * We use a Map here because the traversals can be ordered slightly differently
     */
    def transIrr(p: Producer[P, Any]): Map[Any, Int] =
      (p :: Producer.transitiveDependenciesOf(p))
        .map(irreducible)
        .collect { case Some(irr) => irr }
        .groupBy(identity)
        .mapValues(_.size)

    val dependants = Dependants(tail)
    val newDependants = Dependants(newTail)

    /**
     * Each bag of irreducibles can point to more than one node because not all nodes have anything
     * irreducible (such as NamedProducer, IdentityKeyedProducer, MergedProducer, etc...)
     */
    val oldIrrToNode: Map[Map[Any, Int], List[Producer[P, Any]]] = dependants.nodes.groupBy(transIrr)

    /**
     * Basically do a graph walk on the list of irreducibles for each node
     */
    val newNames: Map[Producer[P, Any], List[String]] = newDependants.nodes.map { n =>
      val newNodeIrr = transIrr(n)
      oldIrrToNode.get(newNodeIrr) match {
        case Some(oldProdList) => // get the name in the original graph
          // Find the longest list of names
          val oldNames = oldProdList.map(dependants.namesOf(_)).maxBy(_.size)
          n -> oldNames.map(_.id)
        case None =>
          val newLine = "\n"
          sys.error(s"Node $n in the new node has no corresponding node in the original graph: ${tail}.\n" +
            s"new: ${newNodeIrr}\n" +
            s"old: ${oldIrrToNode.mkString(newLine)}")
      }
    }(breakOut)
    (newNames, newTail)
  }
}
