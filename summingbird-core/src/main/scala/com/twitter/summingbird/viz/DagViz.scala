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

package com.twitter.summingbird.viz

import com.twitter.summingbird._
import com.twitter.summingbird.planner._

case class DagViz[P <: Platform[P]](dag: Dag[P]) {
  type BaseLookupTable[T] = (Map[T, String], Map[String, Int])
  type NameLookupTable = BaseLookupTable[Producer[P, _]]

  val dependantState: Dependants[P] = Dependants(dag.tail)
  def buildLookupTable[T]() = (Map[T, String](), Map[String, Int]())
  def emptyNameLookupTable(): NameLookupTable = buildLookupTable()
  private def defaultName[T](node: T): String = node.getClass.getName.replaceFirst("com.twitter.summingbird.", "")

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

  def getSubGraphStr(nameLookupTable: NameLookupTable, node: Node[P]): (List[String], List[String], NameLookupTable) = {
    node.members.foldLeft((List[String](), List[String](), nameLookupTable)) {
      case ((definitions, mappings, nameLookupTable), nextNode) =>
        val dependants = dependantState.dependantsOf(nextNode).getOrElse(Set())

        val (updatedLookupTable, uniqueNodeName) = getName(nameLookupTable, nextNode)
        val nodeName = "\"" + uniqueNodeName + "\""

        val (newMappings, innerNameLookupTable) = dependants.foldLeft((List[String](), updatedLookupTable)) {
          case ((mappings, innerNameLookupTable), childNode) =>

            val (updatedLookupTable2, uniqueChildName) = getName(innerNameLookupTable, childNode)
            val childName = "\"" + uniqueChildName + "\""
            val mappingStr = "%s -> %s\n".format(nodeName, childName)

            (mappingStr :: mappings, updatedLookupTable2)
        }
        (nodeName :: definitions, newMappings ++ mappings, innerNameLookupTable)
    }
  }
  def genClusters(): String = {
    val (clusters, producerMappings, producerNames, nodeToShortLookupTable) = dag.nodes.foldLeft((List[String](), List[String](), emptyNameLookupTable(), Map[Node[P], String]())) {
      case ((clusters, producerMappings, nameLookupTable, nodeShortName), node) =>

        val (nodeDefinitions, mappings, newNameLookupTable) = getSubGraphStr(nameLookupTable, node)
        val shortName = "cluster_" + node.hashCode.toHexString
        val newNodeShortName = nodeShortName + (node -> shortName)
        val nextCluster = "subgraph %s {\n\tlabel=\"%s\"\n%s\n}\n".format(shortName, dag.getNodeName(node), nodeDefinitions.mkString("\n"))
        (nextCluster :: clusters, mappings ++ producerMappings, newNameLookupTable, newNodeShortName)
    }

    val clusterMappings = dag.nodes.flatMap {
      case node =>
        dag.dependantsOf(node).collect { case n => "cluster_%s -> cluster_%s [style=dashed]".format(node.hashCode.toHexString, n.hashCode.toHexString) }
    }

    "digraph summingbirdGraph {\n" + (clusters ++ producerMappings ++ clusterMappings).mkString("\n") + "\n}"
  }

  override def toString(): String = genClusters
}
