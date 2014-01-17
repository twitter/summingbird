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
import scala.collection.mutable.{Map => MMap}

case class ProducerViz[P <: Platform[P]](tail: Producer[P, _]) {
  private val dependantState = Dependants(tail)
  private val nodeLookupTable = MMap[Producer[P, _], String]()
  private val nameLookupTable = MMap[String, Int]()

  def getName(node: Producer[P, _]): String = {
    val preferredName = node match {
      case NamedProducer(parent, name) => "NamedProducer(%s)".format(name)
      case _ => node.getClass.getName.replaceFirst("com.twitter.summingbird.", "")
    }

    nodeLookupTable.get(node) match {
      case Some(name) => name
      case None =>
        nameLookupTable.get(preferredName) match {
          case Some(count) => {
            val newNum = count + 1
            val newName = preferredName + "[" + newNum + "]"
            nodeLookupTable += (node -> newName)
            nameLookupTable += (preferredName -> newNum)
            newName
          }
          case None =>
            nodeLookupTable +=  (node -> preferredName)
            nameLookupTable += (preferredName -> 1)
            preferredName
        }
    }
  }

  override def toString() : String = {
    val base = "digraph summingbirdGraph {\n"
    println(dependantState.nodes.size)
    val graphStr = dependantState.nodes.foldLeft("") { case (runningStr, nextNode) =>
      val evalNode = nextNode
      val children = dependantState.dependantsOf(evalNode).getOrElse(List[Producer[P, _]]())
      val nodeName = getName(evalNode)
      val new_str = children.foldLeft(""){ case (innerRunningStr, c)  =>
        val innerNewStr = "\"" + nodeName + "\" -> \""
        val pChildName = getName(c)

        val innerNewStr2 = pChildName + "\"\n"
        innerRunningStr + innerNewStr + innerNewStr2
      }
      runningStr + new_str
    }
    base + graphStr + "\n}"
  }
}
