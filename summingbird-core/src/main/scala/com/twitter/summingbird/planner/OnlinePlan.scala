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

class OnlinePlan[P <: Platform[P], V](tail: Producer[P, V]) {
  private type Prod[T] = Producer[P, T]
  private type VisitedStore = Set[Prod[_]]
  private type CNode = Node[P]
  private type CFlatMapNode = FlatMapNode[P] 

  private val dep = Dependants(tail)
  private val forkedNodes = Producer.transitiveDependenciesOf(tail)
                        .filter(dep.fanOut(_).exists(_ > 1)).toSet
  private def distinctAddToList[T](l : List[T], n : T): List[T] = if(l.contains(n)) l else (n :: l)

  private def mergableWithSource(dep: Producer[P, _]): Boolean = {
    dep match {
      case NamedProducer(producer, _) => true
      case IdentityKeyedProducer(producer) => true
      case OptionMappedProducer(producer, _) => true
      case Source(_) => true
      case _ => false
    }
  } 

  private def allDepsMergeableWithSource(p: Prod[_]): Boolean = mergableWithSource(p) && Producer.dependenciesOf(p).forall(allDepsMergeableWithSource)

  // Add the dependentProducer to a Node along with each of its dependencies in turn.
  private def addWithDependencies[T](dependantProducer: Prod[T], previousBolt: CNode,
                                  akkaRegistry: List[CNode], visited: VisitedStore) : (List[CNode], VisitedStore) = {

    if (visited.contains(dependantProducer)) {
      (distinctAddToList(akkaRegistry, previousBolt), visited)
    } else {
      val currentBolt = previousBolt.add(dependantProducer)
      val visitedWithN = visited + dependantProducer

      def recurse[U](
        producer: Prod[U],
        updatedBolt: CNode = currentBolt,
        updatedRegistry: List[CNode] = akkaRegistry,
        visited: VisitedStore = visitedWithN)
      : (List[CNode], VisitedStore) = {
        addWithDependencies(producer, updatedBolt, updatedRegistry, visited)
      }

      /*
       * The purpose of this method is to see if we need to add a new physical node to the graph,
       * or if we can continue by adding this producer to the current physical node.
       *
       * This function acts as a look ahead, rather than depending on the state of the current node it depends
       * on the nodes further along in the dag. That is conditions for spliting into multiple Nodes are based on as yet
       * unvisisted Producers.
       */
      def maybeSplitThenRecurse[U, A](currentProducer: Prod[U], dep: Prod[A]): (List[CNode], VisitedStore) = {
        val doSplit = dep match {
          case _ if (forkedNodes.contains(dep)) => true
          // If we are a flatmap, but there haven't been any other flatmaps yet(i.e. the registry is of size 1, the summer).
          // Then we must split to avoid a 2 level higherarchy
          case _ if (currentBolt.isInstanceOf[FlatMapNode[_]] && akkaRegistry.size == 1 && allDepsMergeableWithSource(dep)) => true
          case _ if ((!mergableWithSource(currentProducer)) && allDepsMergeableWithSource(dep)) => true
          case _ => false
        }
        if (doSplit) {
          recurse(dep, updatedBolt = FlatMapNode(), updatedRegistry = distinctAddToList(akkaRegistry, currentBolt))
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
            if(subL == subR) throw new Exception("Online Planner doesn't support both the left and right sides of a join being the same node.")
            val (lMergeNodes, lSiblings) = mergeCollapse(subL)
            val (rMergeNodes, rSiblings) = mergeCollapse(subR)
            (distinctAddToList((lMergeNodes ::: rMergeNodes).distinct, p), (lSiblings ::: rSiblings).distinct)
          case _ => (List(), List(p))
        }
      }

      dependantProducer match { 
        case Summer(producer, _, _) => recurse(producer, updatedBolt = FlatMapNode(), updatedRegistry = distinctAddToList(akkaRegistry, currentBolt.toSummer))
        case IdentityKeyedProducer(producer) => maybeSplitThenRecurse(dependantProducer, producer)
        case NamedProducer(producer, newId) => maybeSplitThenRecurse(dependantProducer, producer)
        case Source(spout) => (distinctAddToList(akkaRegistry, currentBolt.toSource), visitedWithN)
        case OptionMappedProducer(producer, op) => maybeSplitThenRecurse(dependantProducer, producer)
        case FlatMappedProducer(producer, op)  => maybeSplitThenRecurse(dependantProducer, producer)
        case WrittenProducer(producer, sinkSupplier)  => maybeSplitThenRecurse(dependantProducer, producer)
        case LeftJoinedProducer(producer, _) => maybeSplitThenRecurse(dependantProducer, producer)
        case MergedProducer(l, r) =>
          // TODO support de-duping self merges  https://github.com/twitter/summingbird/issues/237
          if(l == r) throw new Exception("Online Planner doesn't support both the left and right sides of a join being the same node.")
          val (otherMergeNodes, dependencies) = mergeCollapse(dependantProducer)
          val newCurrentBolt = otherMergeNodes.foldLeft(currentBolt)(_.add(_))
          val visitedWithOther = otherMergeNodes.foldLeft(visitedWithN){ (visited, n) => visited + n }

          // Recurse down all the newly generated dependencies
          dependencies.foldLeft((distinctAddToList(akkaRegistry, newCurrentBolt), visitedWithOther)) { case ((newAkkaReg, newVisited), n) =>
            recurse(n, FlatMapNode(), newAkkaReg, newVisited)
          }
      }
    }
  }

  // This takes an initial pass through all of the Producers, assigning them to Nodes
  private def buildNodesSet: List[CNode] = {
    val (akkaRegistry, _) = addWithDependencies(tail, FlatMapNode(), List[CNode](), Set())
    akkaRegistry
  }
}

object OnlinePlan {
  def apply[P <: Platform[P], T](tail: Producer[P, T]): Dag[P] = {
    val planner = new OnlinePlan(tail)
    val akkaNodeSet: List[Node[P]] = planner.buildNodesSet

    // The nodes are added in a source -> summer way with how we do list prepends
    // but its easier to look at laws in a summer -> source manner
    // We also drop all Nodes with no members(may occur when we visit a node already seen and its the first in that Node)
    val reversedNodeSet = akkaNodeSet.filter(_.members.size > 0).foldLeft(List[Node[P]]()){(nodes, n) => n.reverse :: nodes}
    Dag(tail, reversedNodeSet)
  }  
}
