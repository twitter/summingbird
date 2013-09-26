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

case class NodeIdentifier(identifier: String) {
  override def toString: String = identifier
}

sealed trait Node[P <: Platform[P]] {
  val members: List[Producer[P, _]] = List()

  private val dependantStateOpt = members.headOption.map(h => Dependants(h))

  def dependantsOf(p: Producer[P, _]): List[Producer[P, _]] = {
    dependantStateOpt match {
      case Some(dependantState) => dependantState.dependantsOf(p).getOrElse(List())
      case _ => List()
    }
  }

  def localDependantsOf(p: Producer[P, _]): List[Producer[P, _]] = dependantsOf(p).filter(members.contains(_))

  def toSpout: SourceNode[P] = SourceNode(this.members)

  def toSummer: SummerNode[P] = SummerNode(this.members)

  def contains(p: Producer[P, _]): Boolean = members.contains(p)

  def getNameFallback: String = getClass.getName.replaceFirst("com.twitter.summingbird.storm.", "")

  def getName(dag: Dag[P]): String = dag.getNodeName(this)

  def collapseNamedNodes: String = {
    val membersCombined = members.reverse.collect { case NamedProducer(_, n) => n.replace("-", "=") }.mkString(",")
    if (membersCombined.size > 0) "(" + membersCombined + ")" else ""
  }

  def shortName(): NodeIdentifier

  def add(node: Producer[P, _]): Node[P]

  def reverse: Node[P]

  def toStringWithPrefix(prefix: String): String = {
    prefix + getNameFallback + "\n" + members.foldLeft("") {
      case (str, producer) =>
        str + prefix + "\t" + producer.getClass.getName.replaceFirst("com.twitter.summingbird.", "") + "\n"
    }
  }

  override def toString(): String = {
    toStringWithPrefix("\t")
  }

}

// This is the default state for Nodes if there is nothing special about them.
// There can be an unbounded number of these and there is no hard restrictions on ordering/where. Other than
// locations which must be one of the others
case class FlatMapNode[P <: Platform[P]](override val members: List[Producer[P, _]] = List()) extends Node[P] {
  def add(node: Producer[P, _]): Node[P] = if (members.contains(node)) this else this.copy(members = node :: members)
  def reverse = this.copy(members.reverse)
  override def shortName(): NodeIdentifier = NodeIdentifier("FlatMap" + collapseNamedNodes)
}

case class SummerNode[P <: Platform[P]](override val members: List[Producer[P, _]] = List()) extends Node[P] {
  def add(node: Producer[P, _]): Node[P] = if (members.contains(node)) this else this.copy(members = node :: members)
  def reverse = this.copy(members.reverse)
  override def shortName(): NodeIdentifier = NodeIdentifier("Summer" + collapseNamedNodes)
}

case class SourceNode[P <: Platform[P]](override val members: List[Producer[P, _]] = List()) extends Node[P] {
  def add(node: Producer[P, _]): Node[P] = if (members.contains(node)) this else this.copy(members = node :: members)
  def reverse = this.copy(members.reverse)
  override def shortName(): NodeIdentifier = NodeIdentifier("Source" + collapseNamedNodes)
}

case class Dag[P <: Platform[P]](tail: Producer[P, _], producerToNode: Map[Producer[P, _], Node[P]],
  nodes: List[Node[P]],
  nodeToName: Map[Node[P], String] = Map[Node[P], String](),
  nameToNode: Map[String, Node[P]] = Map[String, Node[P]](),
  dependenciesOfM: Map[Node[P], List[Node[P]]] = Map[Node[P], List[Node[P]]](),
  dependantsOfM: Map[Node[P], List[Node[P]]] = Map[Node[P], List[Node[P]]]()) {
  def connect(src: Node[P], dest: Node[P]): Dag[P] = {
    if (src == dest) {
      this
    } else {
      assert(!dest.isInstanceOf[SourceNode[_]])
      // We build/maintain two maps,
      // Nodes to which each node depends on
      // and nodes on which each node depends
      val oldSrcDependants = dependantsOfM.getOrElse(src, List[Node[P]]())
      val newSrcDependants = if(oldSrcDependants.contains(dest)) oldSrcDependants else (dest :: oldSrcDependants)
      val newDependantsOfM = dependantsOfM + (src -> newSrcDependants)
      
      val oldDestDependencies = dependenciesOfM.getOrElse(dest, List[Node[P]]())
      val newDestDependencies = if(oldDestDependencies.contains(src)) oldDestDependencies else (src :: oldDestDependencies)
      val newDependenciesOfM = dependenciesOfM + (dest -> newDestDependencies)
      
      copy(dependenciesOfM = newDependenciesOfM, dependantsOfM = newDependantsOfM)
    }
  }

  def locateOpt(p: Producer[P, _]): Option[Node[P]] = producerToNode.get(p)
  def locate(p: Producer[P, _]): Node[P] = locateOpt(p).get
  def connect(src: Producer[P, _], dest: Producer[P, _]): Dag[P] = connect(locate(src), locate(dest))

  def getNodeName(n: Node[P]): String = nodeToName(n)
  def tailN: Node[P] = producerToNode(tail)

  def dependantsOf(n: Node[P]): List[Node[P]] = dependantsOfM.get(n).getOrElse(List())
  def dependenciesOf(n: Node[P]): List[Node[P]] = dependenciesOfM.get(n).getOrElse(List())

  def toStringWithPrefix(prefix: String): String = {
    prefix + "Dag\n" + nodes.foldLeft("") {
      case (str, node) =>
        str + node.toStringWithPrefix(prefix + "\t") + "\n"
    }
  }

  override def toString(): String = toStringWithPrefix("\t")
}

object Dag {
   def apply[P <: Platform[P], T](tail: Producer[P, _], registry: List[Node[P]]): Dag[P] = {
    def buildProducerToNodeLookUp(stormNodeSet: List[Node[P]]): Map[Producer[P, _], Node[P]] = {
      stormNodeSet.foldLeft(Map[Producer[P, _], Node[P]]()) { (curRegistry, stormNode) =>
        stormNode.members.foldLeft(curRegistry) { (innerRegistry, producer) =>
          (innerRegistry + (producer -> stormNode))
        }
      }
    }
    val producerToNode = buildProducerToNodeLookUp(registry)
    val dag = registry.foldLeft(Dag(tail, producerToNode, registry)) { (curDag, stormNode) =>
      // Here we are building the Dag's connection topology.
      // We visit every producer and connect the Node's represented by its dependant and dependancies.
      // Producers which live in the same node will result in a NOP in connect.
      stormNode.members.foldLeft(curDag) { (innerDag, dependantProducer) =>
        Producer.dependenciesOf(dependantProducer)
          .foldLeft(innerDag) { (dag, dep) => dag.connect(dep, dependantProducer) }
      }
    }

    def tryGetName(name: String, seen: Set[String], indxOpt: Option[Int] = None): String = {
      indxOpt match {
        case None => if (seen.contains(name)) tryGetName(name, seen, Some(1)) else name
        case Some(indx) => if (seen.contains(name + "." + indx)) tryGetName(name, seen, Some(indx + 1)) else name + "." + indx
      }
    }

    def genNames(dep: Node[P], dag: Dag[P], outerNodeToName: Map[Node[P], String], usedNames: Set[String]): (Map[Node[P], String], Set[String]) = {
      dag.dependenciesOf(dep).foldLeft((outerNodeToName, usedNames)) {
        case ((nodeToName, taken), n) =>
          val name = tryGetName(nodeToName(dep) + "-" + n.shortName, taken)
          val useName = nodeToName.get(n) match {
            case None => name
            case Some(otherName) => if (otherName.split("-").size > name.split("-").size) name else otherName
          }
          genNames(n, dag, nodeToName + (n -> useName), taken + useName)
      }
    }

    val (nodeToName, _) = genNames(dag.tailN, dag, Map(dag.tailN -> "Tail"), Set("Tail"))
    val nameToNode = nodeToName.map((t) => (t._2, t._1))
    dag.copy(nodeToName = nodeToName, nameToNode = nameToNode)
  }
}