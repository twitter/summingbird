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

package com.twitter.summingbird.storm.viz

import java.io.Writer
import com.twitter.summingbird.{Platform, Producer, Dependants, NamedProducer, IdentityKeyedProducer}
import com.twitter.summingbird.storm._

case class VizGraph(dag: StormDag, dependantState: Dependants[Storm]) {
  type BaseLookupTable[T] = (Map[T, String], Map[String, Int])
  type NameLookupTable = BaseLookupTable[Producer[Storm, _]]

  type NodeNameLookupTable = BaseLookupTable[StormNode]
  def buildLookupTable[T]() = (Map[T, String](), Map[String, Int]())
  def emptyNodeNameLookupTable(): NodeNameLookupTable = buildLookupTable()
  def emptyNameLookupTable(): NameLookupTable = buildLookupTable()

  def getName[T](curLookupTable: BaseLookupTable[T], node: T, requestedName: Option[String] = None): (BaseLookupTable[T], String) = {
    val nodeLookupTable = curLookupTable._1
    val nameLookupTable = curLookupTable._2
    val preferredName = requestedName.getOrElse(node.getClass.getName.replaceFirst("com.twitter.summingbird.",""))
    nodeLookupTable.get(node) match {
      case Some(name) => (curLookupTable, name)
      case None => {
        nameLookupTable.get(preferredName) match {
          case Some(count) => {
            val newNum = count + 1
            val newName = preferredName + "[" + newNum + "]"
            (((nodeLookupTable + (node -> newName)), (nameLookupTable + (preferredName -> newNum))), newName)
          }
          case None => {
            (((nodeLookupTable + (node -> preferredName)), (nameLookupTable + (preferredName -> 1))), preferredName)
          }
        }
      }
    }
  }

  def getSubGraphStr(nameLookupTable: NameLookupTable, node: StormNode): (List[String], List[String], NameLookupTable) = {
    node.members.foldLeft((List[String](), List[String](), nameLookupTable)) { case ((definitions, mappings, nameLookupTable), nextNode) =>
      val dependants = dependantState.dependantsOf(nextNode).getOrElse(Set())

      val (updatedLookupTable, uniqueNodeName) = getName(nameLookupTable, nextNode)
      val nodeName = "\"" + uniqueNodeName + "\""

      val (newMappings, innerNameLookupTable) = dependants.foldLeft((List[String](), updatedLookupTable)){ case ((mappings, innerNameLookupTable), childNode)  =>


        val (updatedLookupTable2, uniqueChildName) = getName(innerNameLookupTable, childNode)
        val childName = "\"" + uniqueChildName + "\""

        val mappingStr = "%s -> %s\n".format(nodeName, childName)
        (mappingStr :: mappings, updatedLookupTable2)
      }
      (nodeName :: definitions, newMappings ++ mappings, innerNameLookupTable)
    }
  }
  def genClusters(): (String, Map[StormNode, String]) = {
    val (clusters, producerMappings, nodeNames, producerNames, nodeToShortLookupTable) = dag.nodes.foldLeft((List[String](), List[String](), emptyNodeNameLookupTable(), emptyNameLookupTable(), Map[StormNode, String]())) { 
        case ((clusters, producerMappings, curNodeNameLookupTable, nameLookupTable, nodeShortName), node) =>

          val (newNodeNameLookupTable, nodeName) = getName(curNodeNameLookupTable, node, Some(node.getName))
          val (nodeDefinitions, mappings, newNameLookupTable) = getSubGraphStr(nameLookupTable, node)
          val shortName = "cluster_" + node.hashCode.toHexString
          val newNodeShortName = nodeShortName + (node -> shortName)
          val nextCluster = "subgraph %s {\n\tlabel=\"%s\"\n%s\n}\n".format(shortName, nodeName, nodeDefinitions.mkString("\n"))
          (nextCluster :: clusters, mappings ++ producerMappings, newNodeNameLookupTable, newNameLookupTable, newNodeShortName)
    }

    ((clusters ++ producerMappings).mkString("\n"), nodeToShortLookupTable)

  }
  
  override def toString() : String = {
    val base = "digraph summingbirdGraph {\n"
    val (clusterStr, mapping) = genClusters
    base + clusterStr + "\n}"
  }
}

object StormViz {
  def apply(storm: Storm, tail: Producer[Storm, _], writer: Writer):Unit = {
   
    val dep = Dependants(tail)
    val fanOutSet =
      Producer.transitiveDependenciesOf(tail)
        .filter(dep.fanOut(_).exists(_ > 1)).toSet
    val topoBuilder = new StormToplogyBuilder(tail)
    val (stormRegistry, _) = topoBuilder.collectPass(tail, IntermediateFlatMapStormBolt(), StormRegistry(), fanOutSet, Set())
    val stormDag = StormDag.build(stormRegistry)
    writer.write(VizGraph(stormDag, dep).toString)
  }
}
