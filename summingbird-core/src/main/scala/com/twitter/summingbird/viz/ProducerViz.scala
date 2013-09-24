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

case class ProducerViz[P <: Platform[P]](tail: Producer[P, _]) {
  private val dependantState = Dependants(tail)
  private type NameLookupTable = (Map[Producer[P, _], String], Map[String, Int])
  private val emptyNameLookupTable = (Map[Producer[P, _], String](), Map[String, Int]())

  @annotation.tailrec
  private def recurseGetNode(n :Producer[P, _], nameOpt: Option[String] = None): (String, Producer[P, _], List[Producer[P, _]])  = {
    val children: List[Producer[P, _]] = dependantState.dependantsOf(n).getOrElse(List[Producer[P, _]]())
    val name = nameOpt.getOrElse(n.getClass.getName.replaceFirst("com.twitter.summingbird.", ""))
    children.headOption match {
      case Some(child: NamedProducer[_, _]) =>
        recurseGetNode(child, Some(child.id))
      case _ =>
        (name, n, children)
    }
  }

  def getName(curLookupTable: NameLookupTable, node: Producer[P, _], preferredName: String): (NameLookupTable, String) = {
    val (nodeLookupTable, nameLookupTable) = curLookupTable

    nodeLookupTable.get(node) match {
      case Some(name) => (curLookupTable, name)
      case None => 
        nameLookupTable.get(preferredName) match {
          case Some(count) => {
            val newNum = count + 1
            val newName = preferredName + "[" + newNum + "]"
            (((nodeLookupTable + (node -> newName)), (nameLookupTable + (preferredName -> newNum))), newName)
          }
          case None => (((nodeLookupTable + (node -> preferredName)), (nameLookupTable + (preferredName -> 1))), preferredName)
        }
    }
  }
  override def toString() : String = {
    val base = "digraph summingbirdGraph {\n"
    val (graphStr, _) = dependantState.nodes.foldLeft(("", emptyNameLookupTable)) { case ((runningStr, nameLookupTable), nextNode) =>
      nextNode match {
        case NamedProducer(parent, name) => (runningStr, nameLookupTable)
        case _ => 
          // Compute the lines and new names for the nextNode
          val (rawNodeName, evalNode, children) = recurseGetNode(nextNode)
          val (updatedLookupTable, nodeName) = getName(nameLookupTable, evalNode, rawNodeName)

          val (new_str, innerNameLookupTable) = children.foldLeft(("", updatedLookupTable)){ case ((innerRunningStr, innerNameLookupTable), c)  =>
            val (childName, childNode, _) = recurseGetNode(c)

            val innerNewStr = "\"" + nodeName + "\" -> \"" 
            val (updatedLookupTable2, pChildName) = getName(innerNameLookupTable, childNode, childName)

            val innerNewStr2 = pChildName + "\"\n"
            (innerRunningStr + innerNewStr + innerNewStr2, updatedLookupTable2)
          }
          (runningStr + new_str, innerNameLookupTable)
      }
    }
    base + graphStr + "\n}"
  }
}
