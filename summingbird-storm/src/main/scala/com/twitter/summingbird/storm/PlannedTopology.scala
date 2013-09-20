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

import backtype.storm.LocalCluster
import backtype.storm.Testing
import backtype.storm.testing.CompleteTopologyParam
import backtype.storm.testing.MockedSources
import backtype.storm.tuple.Fields
import backtype.storm.{Config, StormSubmitter}
import backtype.storm.generated.StormTopology
import backtype.storm.topology.{ BoltDeclarer, TopologyBuilder }
import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.storm.option.{ AnchorTuples, IncludeSuccessHandler }
import com.twitter.summingbird.util.CacheSize
import com.twitter.summingbird.kryo.KryoRegistrationHelper
import com.twitter.tormenta.spout.Spout
import com.twitter.summingbird._
import com.twitter.util.Future

import Constants._
import scala.annotation.tailrec

sealed trait StormNode {
  val members: Set[Producer[Storm, _]] = Set()

  private val dependantStateOpt = members.headOption.map(h => Dependants(h))

  def dependantsOf(p: Producer[Storm, _]): List[Producer[Storm, _]] = {
    dependantStateOpt match {
      case Some(dependantState) => dependantState.dependantsOf(p).getOrElse(List())
      case _ => List()
    }
  }

  def localDependantsOf(p: Producer[Storm, _]): List[Producer[Storm, _]] = dependantsOf(p).filter(members.contains(_))

  def toSpout: StormSpout = StormSpout(this.members)

  def toSummer: SummerStormBolt = SummerStormBolt(this.members)

  def contains(p: Producer[Storm, _]): Boolean = members.contains(p)

  def getName(): String = getClass.getName.replaceFirst("com.twitter.summingbird.storm.","")

  def add(node: Producer[Storm, _]): StormNode
}

case class IntermediateFlatMapStormBolt(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Intermediate Flatmap Bolt"
}

case class FinalFlatMapStormBolt(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Final Flatmap Bolt"
}

case class SummerStormBolt(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Summer Bolt"
}

case class StormSpout(override val members: Set[Producer[Storm, _]] = Set()) extends StormNode {
  def add(node: Producer[Storm, _]): StormNode = {
    val newMembers = members + node
    this.copy(members=newMembers)
  }
  override def getName = "Spout"
}


case class StormDag(nodeLut: Map[Producer[Storm, _], StormNode], dag: Map[StormNode, Set[StormNode]] = Map[StormNode, Set[StormNode]](), allNodes: Set[StormNode] = Set[StormNode]()) {
  def connect(src: StormNode, dest: StormNode): StormDag = {
    if (src == dest) {
      this
    } else {
      assert(!dest.isInstanceOf[StormSpout])
      val currentTargets = dag.getOrElse(src, Set[StormNode]())
      StormDag(nodeLut, dag + (src -> (currentTargets + dest) ), allNodes)
    }
  }

  def nodes = allNodes

  def connect(src: Producer[Storm, _], dest: Producer[Storm, _]): StormDag = {
    val newDag = for {
      lNode <- nodeLut.get(src);
      rNode <- nodeLut.get(dest)
    } yield connect(lNode, rNode)
    newDag.getOrElse(this)
  }
  def dependantsOf(n: StormNode): Set[StormNode] = dag.get(n).getOrElse(Set())
}
object StormDag {
  def build(registry: StormRegistry) : StormDag = {
    val nodeLut = registry.buildLut
    registry.registry.foldLeft(StormDag(nodeLut, allNodes = registry.registry)){ (curDag, stormNode) =>
      stormNode.members.foldLeft(curDag) { (innerDag, outerProducer) =>
        outerProducer match {
          case Summer(producer, _, _) => innerDag.connect(producer, outerProducer)
          case IdentityKeyedProducer(producer) => innerDag.connect(producer, outerProducer)
          case NamedProducer(producer, newId) => innerDag.connect(producer, outerProducer)
          case OptionMappedProducer(producer, op, manifest) => innerDag.connect(producer, outerProducer)
          case FlatMappedProducer(producer, op) => innerDag.connect(producer, outerProducer)
          case WrittenProducer(producer, sinkSupplier) => innerDag.connect(producer, outerProducer)
          case LeftJoinedProducer(producer, StoreWrapper(newService)) => innerDag.connect(producer, outerProducer)
          case MergedProducer(l, r) => innerDag.connect(l, outerProducer).connect(r, outerProducer)
          case Source(_, _) => innerDag
        }
      }
    }
  }
}

case class StormRegistry(registry: Set[StormNode] = Set[StormNode]()) {
  
  def register(n: StormNode): StormRegistry =  {
    StormRegistry(registry + n )
  }

  def buildLut() : Map[Producer[Storm, _], StormNode] = {
    registry.foldLeft(Map[Producer[Storm, _], StormNode]()){ (curRegistry, stormNode) =>
      stormNode.members.foldLeft(curRegistry) { (innerRegistry, producer) =>
        (innerRegistry + (producer -> stormNode))
      }
    }
  }
}

class StormToplogyBuilder[P](tail: Producer[Storm, P]) {
  private type Prod[T] = Producer[Storm, T]
  private type VisitedStore = Set[Prod[_]]


  def collectPass[T](outerProducer: Prod[T], previousBolt: StormNode, stormRegistry: StormRegistry,
                      forkedNodes: Set[Prod[_]], visited: VisitedStore): (StormRegistry, VisitedStore) = {
    
    val currentBolt = previousBolt.add(outerProducer)

    def recurse[U](
      producer: Prod[U],
      updatedBolt: StormNode = currentBolt,
      updatedDag: StormRegistry = stormRegistry,
      visited: VisitedStore = visited)
    : (StormRegistry, VisitedStore) = {
      collectPass(producer, updatedBolt, updatedDag, forkedNodes, visited = visited)
    }

    def maybeSplit[A](dependency: Prod[A], visited: VisitedStore): (StormRegistry, VisitedStore) = {
      if (forkedNodes.contains(dependency)) {
        recurse(dependency, updatedBolt = IntermediateFlatMapStormBolt(), updatedDag = stormRegistry.register(currentBolt), visited = visited)
      } else recurse(dependency, visited = visited)
    }

    def mergeCollapse[A](l: Prod[A], r: Prod[A]): (List[Prod[A]], List[Prod[A]]) = {
      List(l, r).foldLeft((List[Prod[A]](), List[Prod[A]]())) { case ((mergeNodes, sibList), node) =>
        node match {
          case MergedProducer(subL, subR) =>
          val (newMerge, newSib) = mergeCollapse(subL, subR) 
          ((node :: mergeNodes ::: newMerge), sibList ::: newSib)
          case _ => (mergeNodes, node :: sibList)
        }
      }
    }
    
    if (visited.contains(outerProducer)) {
      (stormRegistry, visited)
    } else {
      val visitedWithN = visited + outerProducer
      outerProducer match {
        case Summer(producer, _, _) =>
          recurse(producer, updatedBolt = FinalFlatMapStormBolt(), updatedDag = stormRegistry.register(currentBolt.toSummer), visited = visitedWithN)

        case IdentityKeyedProducer(producer) => maybeSplit(producer, visited = visitedWithN)
        case NamedProducer(producer, newId) => maybeSplit(producer, visited = visitedWithN)
        case Source(spout, manifest) =>
          val spoutBolt = currentBolt.toSpout
          (stormRegistry.register(spoutBolt), visitedWithN)

        case OptionMappedProducer(producer, op, manifest) => maybeSplit(producer, visited = visitedWithN)

        case FlatMappedProducer(producer, op)  => maybeSplit(producer, visited = visitedWithN)

        case WrittenProducer(producer, sinkSupplier)  => maybeSplit(producer, visited = visitedWithN)

        case LeftJoinedProducer(producer, StoreWrapper(newService)) => maybeSplit(producer, visited = visitedWithN)

        case MergedProducer(l, r) =>

          val (mergeNodes, siblings) = mergeCollapse(l, r)
          val newCurrentBolt = mergeNodes.foldLeft(currentBolt)(_.add(_))
          val startingReg = stormRegistry.register(newCurrentBolt)
          siblings.foldLeft((startingReg, visitedWithN)) {case ((newStormReg, newVisited), n) =>
            recurse(n, updatedBolt = IntermediateFlatMapStormBolt(), updatedDag = newStormReg, newVisited)
          }
      }

    }
  }

}