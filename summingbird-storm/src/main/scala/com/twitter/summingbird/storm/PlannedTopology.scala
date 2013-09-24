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

package com.twitter.summingbird.storm

import com.twitter.summingbird._

case class NodeIdentifier(identifier: String) {
  override def toString:String = identifier
}

sealed trait Node {
  val members: List[Producer[Storm, _]] = List()

  private val dependantStateOpt = members.headOption.map(h => Dependants(h))

  def dependantsOf(p: Producer[Storm, _]): List[Producer[Storm, _]] = {
    dependantStateOpt match {
      case Some(dependantState) => dependantState.dependantsOf(p).getOrElse(List())
      case _ => List()
    }
  }

  def localDependantsOf(p: Producer[Storm, _]): List[Producer[Storm, _]] = dependantsOf(p).filter(members.contains(_))

  def toSpout: SourceNode = SourceNode(this.members)

  def toSummer: SummerNode = SummerNode(this.members)

  def contains(p: Producer[Storm, _]): Boolean = members.contains(p)

  def getNameFallback: String = getClass.getName.replaceFirst("com.twitter.summingbird.storm.","")

  def getName(dag: StormDag): String = dag.getNodeName(this)
  
  def collapseNamedNodes:String = {
    val membersCombined = members.reverse.collect{case NamedProducer(_, n) => n.replace("-","=")}.mkString(",")
    if(membersCombined.size > 0 ) "(" + membersCombined + ")" else ""
  }

  def shortName(): NodeIdentifier

  def add(node: Producer[Storm, _]): Node

  def reverse: Node


  def toStringWithPrefix(prefix: String): String = {
    prefix + getNameFallback + "\n" + members.foldLeft(""){ case (str, producer) =>
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
case class FlatMapNode(override val members: List[Producer[Storm, _]] = List()) extends Node {
  def add(node: Producer[Storm, _]): Node = if(members.contains(node)) this else this.copy(members=node :: members)
  def reverse = this.copy(members.reverse)
  override def shortName(): NodeIdentifier = NodeIdentifier("FlatMap" + collapseNamedNodes )
}


case class SummerNode(override val members: List[Producer[Storm, _]] = List()) extends Node {
  def add(node: Producer[Storm, _]): Node = if(members.contains(node)) this else this.copy(members=node :: members)
  def reverse = this.copy(members.reverse)
  override def shortName(): NodeIdentifier = NodeIdentifier("Summer" + collapseNamedNodes)
}

case class SourceNode(override val members: List[Producer[Storm, _]] = List()) extends Node {
  def add(node: Producer[Storm, _]): Node = if(members.contains(node)) this else this.copy(members=node :: members)
  def reverse = this.copy(members.reverse)
  override def shortName(): NodeIdentifier = NodeIdentifier("Source" + collapseNamedNodes)
}



case class StormDag(tail: Producer[Storm, _], producerToNode: Map[Producer[Storm, _], Node],
              nodes: List[Node],
              nodeToName: Map[Node, String] = Map(),
              nameToNode: Map[String, Node] = Map(),
              dependsOnM: Map[Node, List[Node]] = Map[Node, List[Node]](),
              dependantOfM: Map[Node, List[Node]] = Map[Node, List[Node]]()) {
  def connect(src: Node, dest: Node): StormDag = {
    if (src == dest) {
      this
    } else {
      assert(!dest.isInstanceOf[SourceNode])
      // We build/maintain two maps,
      // Nodes to which each node depends on
      // and nodes on which each node depends
      val oldDependsOnTargets = dependsOnM.getOrElse(src, List[Node]())
      val dependsOnTargets = if(oldDependsOnTargets.contains(dest)) oldDependsOnTargets else (dest :: oldDependsOnTargets)
      val oldDependantOfTargets = dependantOfM.getOrElse(dest, List[Node]())
      val dependantOfTargets = if(oldDependantOfTargets.contains(src)) oldDependantOfTargets else (src :: oldDependantOfTargets)

      copy(dependsOnM = dependsOnM + (src -> dependsOnTargets) , dependantOfM = dependantOfM + (dest -> dependantOfTargets))
    }
  }

  def locateOpt(p: Producer[Storm, _]): Option[Node] = producerToNode.get(p)
  def locate(p: Producer[Storm, _]): Node = locateOpt(p).get
  def connect(src: Producer[Storm, _], dest: Producer[Storm, _]): StormDag = connect(locate(src), locate(dest))

  def getNodeName(n: Node): String = nodeToName(n)
  def tailN: Node = producerToNode(tail)

  def dependantsOf(n: Node): List[Node] = dependsOnM.get(n).getOrElse(List())
  def dependsOn(n: Node): List[Node] = dependantOfM.get(n).getOrElse(List())

  def toStringWithPrefix(prefix: String): String = {
    prefix + "StormDag\n" + nodes.foldLeft(""){ case (str, node) =>
      str + node.toStringWithPrefix(prefix + "\t") + "\n"
    }
  }

  override def toString(): String = toStringWithPrefix("\t")
}

object StormDag {
  def buildProducerToNodeLookUp(stormNodeSet: List[Node]) : Map[Producer[Storm, _], Node] = {
    stormNodeSet.foldLeft(Map[Producer[Storm, _], Node]()){ (curRegistry, stormNode) =>
      stormNode.members.foldLeft(curRegistry) { (innerRegistry, producer) =>
        (innerRegistry + (producer -> stormNode))
      }
    }
  }
  def build(tail: Producer[Storm, _], registry: List[Node]) : StormDag = {

    val seenNames = Set[Node]()
    val producerToNode = buildProducerToNodeLookUp(registry)
    val dag = registry.foldLeft(StormDag(tail, producerToNode, registry)){ (curDag, stormNode) =>
      // Here we are building the StormDag's connection topology.
      // We visit every producer and connect the Node's represented by its dependant and dependancies.
      // Producers which live in the same node will result in a NOP in connect.

      stormNode.members.foldLeft(curDag) { (innerDag, dependantProducer) =>
        Producer.dependenciesOf(dependantProducer)
            .foldLeft(innerDag) { (dag, dep) => dag.connect(dep, dependantProducer) }
      }
    }

    def tryGetName(name: String, seen: Set[String], indxOpt: Option[Int] = None) : String = {
      indxOpt match {
        case None => if(seen.contains(name)) tryGetName(name, seen, Some(1)) else name
        case Some(indx) => if(seen.contains(name + "." + indx)) tryGetName(name, seen, Some(indx + 1)) else name + "." + indx
      }
    }

    def genNames(dep: Node, dag: StormDag, outerNodeToName: Map[Node, String], usedNames: Set[String]): (Map[Node, String], Set[String]) = {
      dag.dependsOn(dep).foldLeft((outerNodeToName, usedNames)) {case ((nodeToName, taken), n) =>
          val name = tryGetName(nodeToName(dep) + "-" + n.shortName, taken)
          val useName = nodeToName.get(n) match {
            case None => name
            case Some(otherName) => if(otherName.split("-").size > name.split("-").size) name else otherName
          }
          genNames(n, dag, nodeToName + (n -> useName), taken + useName)
      }
    }

    val (nodeToName, _) = genNames(dag.tailN, dag, Map(dag.tailN -> "Tail"), Set("Tail"))
    val nameToNode = nodeToName.map((t) => (t._2,t._1))
    dag.copy(nodeToName = nodeToName, nameToNode = nameToNode)
  }
}

object DagBuilder {
  private type Prod[T] = Producer[Storm, T]
  private type VisitedStore = Set[Prod[_]]

  def apply[P](tail: Producer[Storm, P]): StormDag = {
    val stormNodeSet = buildNodesSet(tail)

    // The nodes are added in a source -> summer way with how we do list prepends
    // but its easier to look at laws in a summer -> source manner
    // We also drop all Nodes with no members(may occur when we visit a node already seen and its the first in that Node)
    val revsersedNodeSet = stormNodeSet.filter(_.members.size > 0).foldLeft(List[Node]()){(nodes, n) => n.reverse :: nodes}
    StormDag.build(tail, revsersedNodeSet)
  }

  // This takes an initial pass through all of the Producers, assigning them to Nodes
  private def buildNodesSet[P](tail: Producer[Storm, P]): List[Node] = {
    val dep = Dependants(tail)
    val forkedNodes = Producer.transitiveDependenciesOf(tail)
                        .filter(dep.fanOut(_).exists(_ > 1)).toSet
    def distinctAddToList[T](l : List[T], n : T): List[T] = if(l.contains(n)) l else (n :: l)

    // Add the dependentProducer to a Node along with each of its dependencies in turn.
    def addWithDependencies[T](dependantProducer: Prod[T], previousBolt: Node,
                                    stormRegistry: List[Node], visited: VisitedStore) : (List[Node], VisitedStore) = {
      if (visited.contains(dependantProducer)) {
        (distinctAddToList(stormRegistry, previousBolt), visited)
      } else {
        val currentBolt = previousBolt.add(dependantProducer)
        val visitedWithN = visited + dependantProducer

        def recurse[U](
          producer: Prod[U],
          updatedBolt: Node = currentBolt,
          updatedRegistry: List[Node] = stormRegistry,
          visited: VisitedStore = visitedWithN)
        : (List[Node], VisitedStore) = {
          addWithDependencies(producer, updatedBolt, updatedRegistry, visited)
        }

        def mergableWithSource(dep: Prod[_]): Boolean = {
          dep match {
            case NamedProducer(producer, _) => true
            case IdentityKeyedProducer(producer) => true
            case OptionMappedProducer(producer, _) => true
            case Source(_) => true
            case _ => false
          }
        }

        def allDepsMergeableWithSource(p: Prod[_]): Boolean = mergableWithSource(p) && Producer.dependenciesOf(p).forall(allDepsMergeableWithSource)

        /*
         * The purpose of this method is to see if we need to add a new physical node to the graph,
         * or if we can continue by adding this producer to the current physical node.
         *
         * This function acts as a look ahead, rather than depending on the state of the current node it depends
         * on the nodes further along in the dag. That is conditions for spliting into multiple Nodes are based on as yet
         * unvisisted Producers.
         */
        def maybeSplitThenRecurse[U, A](currentProducer: Prod[U], dep: Prod[A]): (List[Node], VisitedStore) = {
          val doSplit = dep match {
            case _ if (forkedNodes.contains(dep)) => true
            // If we are a flatmap, but there haven't been any other flatmaps yet(i.e. the registry is of size 1, the summer).
            // Then we must split to avoid a 2 level higherarchy
            case _ if (currentBolt.isInstanceOf[FlatMapNode] && stormRegistry.size == 1 && allDepsMergeableWithSource(dep)) => true
            case _ if ((!mergableWithSource(currentProducer)) && allDepsMergeableWithSource(dep)) => true
            case _ => false
          }
          if (doSplit) {
            recurse(dep, updatedBolt = FlatMapNode(), updatedRegistry = distinctAddToList(stormRegistry, currentBolt))
            } else {
            recurse(dep)
          }
        }

        /*
         * This is a peek ahead when we meet a MergedProducer. We pull the directly depended on MergedProducer's into the same Node,
         * only if that MergedProducer is not a fan out node.
         * This has the effect of pulling all of the merged streams in as siblings rather than just the two.
         * From this we return a list of the MergedProducers which should be combined into the current Node, and the list of nodes
         * on which these nodes depends (the producers passing data into these MergedProducer).
         */

        def mergeCollapse[A](p: Prod[A]): (List[Prod[A]], List[Prod[A]]) = {
          p match {
            case MergedProducer(subL, subR) if !forkedNodes.contains(p) =>
             // TODO support de-duping self merges  https://github.com/twitter/summingbird/issues/237
              if(subL == subR) throw new Exception("Storm doesn't support both the left and right sides of a join being the same node.")
              val (lMergeNodes, lSiblings) = mergeCollapse(subL)
              val (rMergeNodes, rSiblings) = mergeCollapse(subR)
              (distinctAddToList((lMergeNodes ::: rMergeNodes).distinct, p), (lSiblings ::: rSiblings).distinct)
            case _ => (List(), List(p))
          }
        }

        dependantProducer match {
          case Summer(producer, _, _) => recurse(producer, updatedBolt = FlatMapNode(), updatedRegistry = distinctAddToList(stormRegistry, currentBolt.toSummer))
          case IdentityKeyedProducer(producer) => maybeSplitThenRecurse(dependantProducer, producer)
          case NamedProducer(producer, newId) => maybeSplitThenRecurse(dependantProducer, producer)
          case Source(spout) => (distinctAddToList(stormRegistry, currentBolt.toSpout), visitedWithN)
          case OptionMappedProducer(producer, op) => maybeSplitThenRecurse(dependantProducer, producer)
          case FlatMappedProducer(producer, op)  => maybeSplitThenRecurse(dependantProducer, producer)
          case WrittenProducer(producer, sinkSupplier)  => maybeSplitThenRecurse(dependantProducer, producer)
          case LeftJoinedProducer(producer, StoreWrapper(newService)) => maybeSplitThenRecurse(dependantProducer, producer)
          case MergedProducer(l, r) =>
            // TODO support de-duping self merges  https://github.com/twitter/summingbird/issues/237
            if(l == r) throw new Exception("Storm doesn't support both the left and right sides of a join being the same node.")
            val (otherMergeNodes, dependencies) = mergeCollapse(dependantProducer)
            val newCurrentBolt = otherMergeNodes.foldLeft(currentBolt)(_.add(_))
            val visitedWithOther = otherMergeNodes.foldLeft(visitedWithN){ (visited, n) => visited + n }

            // Recurse down all the newly generated dependencies
            dependencies.foldLeft((distinctAddToList(stormRegistry, newCurrentBolt), visitedWithOther)) { case ((newStormReg, newVisited), n) =>
              recurse(n, FlatMapNode(), newStormReg, newVisited)
            }
        }
      }
    }
    val (stormRegistry, _) = addWithDependencies(tail, FlatMapNode(), List[Node](), Set())
    stormRegistry
  }
}
