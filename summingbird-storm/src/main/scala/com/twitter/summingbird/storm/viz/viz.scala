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

case class VizGraph(dag: StormDag) {
  type BaseLookupTable[T] = (Map[T, String], Map[String, Int])
  type NameLookupTable = BaseLookupTable[Producer[Storm, _]]

  type NodeNameLookupTable = BaseLookupTable[StormNode]
  val dependantState: Dependants[Storm] = Dependants(dag.tail)
  def buildLookupTable[T]() = (Map[T, String](), Map[String, Int]())
  def emptyNodeNameLookupTable(): NodeNameLookupTable = buildLookupTable()
  def emptyNameLookupTable(): NameLookupTable = buildLookupTable()
  private def defaultName[T](node: T): String = node.getClass.getName.replaceFirst("com.twitter.summingbird.","")

  def getName[T](curLookupTable: BaseLookupTable[T], node: T, requestedName: Option[String] = None): (BaseLookupTable[T], String) = {
    val nodeLookupTable = curLookupTable._1
    val nameLookupTable = curLookupTable._2
    val preferredName = requestedName.getOrElse(defaultName(node))
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
  def genClusters(): String = {
    val (clusters, producerMappings, nodeNames, producerNames, nodeToShortLookupTable) = dag.nodes.foldLeft((List[String](), List[String](), emptyNodeNameLookupTable(), emptyNameLookupTable(), Map[StormNode, String]())) { 
        case ((clusters, producerMappings, curNodeNameLookupTable, nameLookupTable, nodeShortName), node) =>

          val (newNodeNameLookupTable, nodeName) = getName(curNodeNameLookupTable, node, Some(node.getName))
          val (nodeDefinitions, mappings, newNameLookupTable) = getSubGraphStr(nameLookupTable, node)
          val shortName = "cluster_" + node.hashCode.toHexString
          val newNodeShortName = nodeShortName + (node -> shortName)
          val nextCluster = "subgraph %s {\n\tlabel=\"%s\"\n%s\n}\n".format(shortName, nodeName, nodeDefinitions.mkString("\n"))
          (nextCluster :: clusters, mappings ++ producerMappings, newNodeNameLookupTable, newNameLookupTable, newNodeShortName)
    }

    "digraph summingbirdGraph {\n" + (clusters ++ producerMappings).mkString("\n") +  "\n}"
  }
  
  override def toString() : String = genClusters
}

object VizGraph {
  def apply(stormDag: StormDag, writer: Writer): Unit = writer.write(apply(stormDag).toString)
  def apply(tail: Producer[Storm, _], writer: Writer): Unit = apply(DagBuilder(tail), writer)
  def apply(tail: Producer[Storm, _]): String = apply(DagBuilder(tail)).toString
}
