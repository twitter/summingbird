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
  type BaseLut[T] = (Map[T, String], Map[String, Int])
  type NameLut = BaseLut[Producer[Storm, _]]

  type NodeNameLut = BaseLut[StormNode]
  def buildLut[T]() = (Map[T, String](), Map[String, Int]())
  def emptyNodeNameLut(): NodeNameLut = buildLut()
  def emptyNameLut(): NameLut = buildLut()

  def getName[T](curLut: BaseLut[T], node: T, requestedName: Option[String] = None): (BaseLut[T], String) = {
    val nodeLUT = curLut._1
    val nameLUT = curLut._2
    val preferredName = requestedName.getOrElse(node.getClass.getName.replaceFirst("com.twitter.summingbird.",""))
    nodeLUT.get(node) match {
      case Some(name) => (curLut, name)
      case None => {
        nameLUT.get(preferredName) match {
          case Some(count) => {
            val newNum = count + 1
            val newName = preferredName + "[" + newNum + "]"
            (((nodeLUT + (node -> newName)), (nameLUT + (preferredName -> newNum))), newName)
          }
          case None => {
            (((nodeLUT + (node -> preferredName)), (nameLUT + (preferredName -> 1))), preferredName)
          }
        }
      }
    }
  }

  def getSubGraphStr(nameLut: NameLut, node: StormNode): (List[String], List[String], NameLut) = {
    node.members.foldLeft((List[String](), List[String](), nameLut)) { case ((definitions, mappings, nameLut), nextNode) =>
      val dependants = dependantState.dependantsOf(nextNode).getOrElse(Set())

      val (updatedLut, uniqueNodeName) = getName(nameLut, nextNode)
      val nodeName = "\"" + uniqueNodeName + "\""

      val (newMappings, innerNameLut) = dependants.foldLeft((List[String](), updatedLut)){ case ((mappings, innerNameLut), childNode)  =>


        val (updatedLut2, uniqueChildName) = getName(innerNameLut, childNode)
        val childName = "\"" + uniqueChildName + "\""

        val mappingStr = "%s -> %s\n".format(nodeName, childName)
        (mappingStr :: mappings, updatedLut2)
      }
      (nodeName :: definitions, newMappings ++ mappings, innerNameLut)
    }
  }
  def genClusters(): (String, Map[StormNode, String]) = {

    val (clusters, producerMappings, nodeNames, producerNames, nodeToShortLut) = dag.nodes.foldLeft((List[String](), List[String](), emptyNodeNameLut(), emptyNameLut(), Map[StormNode, String]())) { 
        case ((clusters, producerMappings, curNodeNameLut, nameLut, nodeShortName), node) =>

          val (newNodeNameLut, nodeName) = getName(curNodeNameLut, node)
          val (nodeDefinitions, mappings, newNameLut) = getSubGraphStr(nameLut, node)
          val shortName = "cluster_" + node.hashCode.toHexString
          val newNodeShortName = nodeShortName + (node -> shortName)
          val nextCluster = "subgraph %s {\n\tlabel=\"%s\"\n%s\n}\n".format(shortName, nodeName, nodeDefinitions.mkString("\n"))
          (nextCluster :: clusters, mappings ++ producerMappings, newNodeNameLut, newNameLut, newNodeShortName)
    }

    ((clusters ++ producerMappings).mkString("\n"), nodeToShortLut)

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
    val (stormRegistry, _) = storm.collectPass(tail, StormBolt(), StormRegistry(), fanOutSet, Set())
    val stormDag = StormDag.build(stormRegistry)
    writer.write(VizGraph(stormDag, dep).toString)
  }
}
