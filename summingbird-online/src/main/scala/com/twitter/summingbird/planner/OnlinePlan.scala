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

  private val depData = Dependants(tail)
  private val forkedNodes = depData.nodes
                            .filter(depData.fanOut(_).exists(_ > 1)).toSet
  private def distinctAddToList[T](l : List[T], n : T): List[T] = if(l.contains(n)) l else (n :: l)

  private def mergableWithSource(dep: Producer[P, _]): Boolean = {
    dep match {
      case NamedProducer(producer, _) => true
      case IdentityKeyedProducer(producer) => true
      case OptionMappedProducer(producer, _) => true
      case Source(_) => true
      case AlsoProducer(_, _) => true
      case FlatMappedProducer(_, _) => false
      case KeyFlatMappedProducer(_, _) => false
      case LeftJoinedProducer(_, _) => false
      case Summer(_, _, _) => false
      case WrittenProducer(_, _) => false
      case MergedProducer(_, _) => false
    }
  }

  private def noOpProducer(dep: Producer[P, _]): Boolean = {
    dep match {
      case NamedProducer(_, _) => true
      case IdentityKeyedProducer(_) => true
      case MergedProducer(_, _) => true
      case AlsoProducer(_, _) => true
      case FlatMappedProducer(_, _) => false
      case KeyFlatMappedProducer(_, _) => false
      case LeftJoinedProducer(_, _) => false
      case OptionMappedProducer(_, _) => false
      case Source(_) => false
      case Summer(_, _, _) => false
      case WrittenProducer(_, _) => false
    }
  }

  private def hasSummerAsDependantProducer(p: Prod[_]): Boolean =
            depData.dependantsOf(p).get.collect { case s: Summer[_, _, _] => s }.headOption.isDefined

  private def dependsOnSummerProducer(p: Prod[_]): Boolean =
            Producer.dependenciesOf(p).collect { case s: Summer[_, _, _] => s }.headOption.isDefined

  private def allDepsMergeableWithSource(p: Prod[_]): Boolean = mergableWithSource(p) && Producer.dependenciesOf(p).forall(allDepsMergeableWithSource)

  // Add the dependentProducer to a Node along with each of its dependencies in turn.
  private def addWithDependencies[T](dependantProducer: Prod[T], previousBolt: CNode,
                                  nodeSet: List[CNode], visited: VisitedStore) : (List[CNode], VisitedStore) = {

    if (visited.contains(dependantProducer)) {
      (distinctAddToList(nodeSet, previousBolt), visited)
    } else {
      val currentBolt = previousBolt.add(dependantProducer)
      val visitedWithN = visited + dependantProducer

      def recurse[U](
        producer: Prod[U],
        updatedBolt: CNode = currentBolt,
        updatedRegistry: List[CNode] = nodeSet,
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
      def maybeSplitThenRecurse[U, A](currentProducer: Prod[U], dep: Prod[A], activeBolt: CNode = currentBolt): (List[CNode], VisitedStore) = {
        val doSplit = activeBolt match {
          case _ if (forkedNodes.contains(dep)) => true // If its forked we must split
          case SummerNode(_) if !noOpProducer(dep) => true // If its not combinable with a Summer we must split
          case _ if (!noOpProducer(currentProducer) && dependsOnSummerProducer(currentProducer)) => true // If its not combinable with a Summer we must split
          // If we need to have a node between a Source and a Summer, inject it here
          case FlatMapNode(_) if hasSummerAsDependantProducer(currentProducer) && allDepsMergeableWithSource(dep) => true
          case _ if ((!mergableWithSource(currentProducer)) && allDepsMergeableWithSource(dep)) => true
          case _ => false
        }
        if (doSplit) {
          recurse(dep, updatedBolt = FlatMapNode(), updatedRegistry = distinctAddToList(nodeSet, activeBolt))
          } else {
          recurse(dep, updatedBolt = activeBolt)
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
        case Summer(producer, _, _) => maybeSplitThenRecurse(dependantProducer, producer, currentBolt.toSummer)
        case IdentityKeyedProducer(producer) => maybeSplitThenRecurse(dependantProducer, producer)
        case NamedProducer(producer, _) => sys.error("Should not try plan a named producer")
        case AlsoProducer(lProducer, rProducer) =>
              val (updatedReg, updatedVisited) = maybeSplitThenRecurse(dependantProducer, rProducer)
              recurse(lProducer, FlatMapNode(), updatedReg, updatedVisited)
        case Source(spout) => (distinctAddToList(nodeSet, currentBolt.toSource), visitedWithN)
        case OptionMappedProducer(producer, _) => maybeSplitThenRecurse(dependantProducer, producer)
        case FlatMappedProducer(producer, _)  => maybeSplitThenRecurse(dependantProducer, producer)
        case KeyFlatMappedProducer(producer, _)  => maybeSplitThenRecurse(dependantProducer, producer)
        case WrittenProducer(producer, _)  => maybeSplitThenRecurse(dependantProducer, producer)
        case LeftJoinedProducer(producer, _) => maybeSplitThenRecurse(dependantProducer, producer)
        case MergedProducer(l, r) =>
          // TODO support de-duping self merges  https://github.com/twitter/summingbird/issues/237
          if(l == r) throw new Exception("Online Planner doesn't support both the left and right sides of a join being the same node.")
          val (otherMergeNodes, dependencies) = mergeCollapse(dependantProducer)
          val newCurrentBolt = otherMergeNodes.foldLeft(currentBolt)(_.add(_))
          val visitedWithOther = otherMergeNodes.foldLeft(visitedWithN){ (visited, n) => visited + n }

          // Recurse down all the newly generated dependencies
          dependencies.foldLeft((distinctAddToList(nodeSet, newCurrentBolt), visitedWithOther)) { case ((newNodeSet, newVisited), n) =>
            recurse(n, FlatMapNode(), newNodeSet, newVisited)
          }
      }
    }
  }

  val (nodeSet, _) = addWithDependencies(tail, FlatMapNode(), List[CNode](), Set())
}

object OnlinePlan {
  def apply[P <: Platform[P], T](tail: TailProducer[P, T]): Dag[P] = {
    val (nameMap, strippedTail) = StripNamedNode(tail)
    val planner = new OnlinePlan(strippedTail)
    val nodesSet = planner.nodeSet

    // The nodes are added in a source -> summer way with how we do list prepends
    // but its easier to look at laws in a summer -> source manner
    // We also drop all Nodes with no members(may occur when we visit a node already seen and its the first in that Node)
    val reversedNodeSet = nodesSet.filter(_.members.size > 0).foldLeft(List[Node[P]]()){(nodes, n) => n.reverse :: nodes}
    Dag(tail, nameMap, strippedTail, reversedNodeSet)
  }
}
