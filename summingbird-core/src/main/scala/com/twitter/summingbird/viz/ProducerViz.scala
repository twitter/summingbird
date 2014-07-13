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
import scala.collection.mutable.{ Map => MMap }

case class ProducerViz[P <: Platform[P]](tail: Producer[P, _]) {
  private val dependantState = Dependants(tail)
  // These are caches that are only kept/used for a short period
  // its single threaded and mutation is tightly controlled.
  // Used here instead of an immutable for simplification of code.
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
            nodeLookupTable += (node -> preferredName)
            nameLookupTable += (preferredName -> 1)
            preferredName
        }
    }
  }

  override def toString(): String = {
    val base = "digraph summingbirdGraph {\n"
    val graphStr = dependantState.nodes.flatMap { evalNode =>
      val children = dependantState.dependantsOf(evalNode).getOrElse(sys.error("Invalid node: %s, unable to find dependants".format(evalNode)))
      val nodeName = getName(evalNode)
      children.map { c =>
        "\"%s\" -> \"%s\"\n".format(nodeName, getName(c))
      }
    }.mkString("")
    base + graphStr + "\n}"
  }
}
