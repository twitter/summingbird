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

import java.io.Writer
import com.twitter.summingbird.{Platform, Producer, Dependants, NamedProducer, IdentityKeyedProducer}


case class VizGraph[P <: Platform[P]](tail: Producer[P, _]) {
  val dependantState = Dependants(tail)
  type NameLut = (Map[Producer[P, _], String], Map[String, Int])
  def emptyNameLut = (Map[Producer[P, _], String](), Map[String, Int]())

  @annotation.tailrec
  private def recurseGetNode(n :Producer[P, _], name: String): (String, Producer[P, _], Set[Producer[P, _]])  = {
    val children: Set[Producer[P, _]] = dependantState.dependantsOf(n).getOrElse(Set[Producer[P, _]]())
    children.headOption match {
      case Some(child: NamedProducer[_, _]) =>
        recurseGetNode(child, child.id)
      case Some(child: IdentityKeyedProducer[_, _, _]) =>
        recurseGetNode(child, name)
      case _ =>
        (name, n, children)
    }
  }

  def getName(curLut: NameLut, node: Producer[P, _], preferredName: String): (NameLut, String) = {
    val nodeLUT = curLut._1
    val nameLUT = curLut._2
    nodeLUT.get(node) match {
      case Some(name) => (curLut, name)
      case None => {
        nameLUT.get(preferredName) match {
          case Some(count) => {
            val newNum = count + 1
            val newName = preferredName + "[" + newNum + "]"
            println(nameLUT)
            (((nodeLUT + (node -> newName)), (nameLUT + (preferredName -> newNum))), newName)
          }
          case None => {
            (((nodeLUT + (node -> preferredName)), (nameLUT + (preferredName -> 1))), preferredName)
          }
        }
      }
    }
  }
  override def toString() : String = {
    
    val base = "digraph summingbirdGraph {\n"
    val (graphStr, _) = dependantState.nodes.foldLeft(("", emptyNameLut)) { case ((runningStr, nameLut), nextNode) =>
      nextNode match {
        case NamedProducer(parent, name) => (runningStr, nameLut)
        case i : IdentityKeyedProducer[_, _, _] => (runningStr, nameLut)
        case _ => {
          val (rawNodeName, evalNode, children) = recurseGetNode(nextNode, nextNode.getClass.toString)
          val (updatedLut, nodeName) = getName(nameLut, evalNode, rawNodeName)

          val (new_str, innerNameLut) = children.foldLeft(("", updatedLut)){ case ((innerRunningStr, innerNameLut), c)  =>
            val (childName, childNode, _) = recurseGetNode(c, c.getClass.toString)

            val innerNewStr = "\"" + nodeName + "(" + dependantState.fanOut(evalNode) + ")" + "\" -> \"" 
            val (updatedLut2, pChildName) = getName(innerNameLut, childNode, childName)

            val innerNewStr2 = pChildName + "(" + dependantState.fanOut(childNode) + ")" + "\"\n"
            (innerRunningStr + innerNewStr + innerNewStr2, updatedLut2)
          }
          (runningStr + new_str, innerNameLut)
        }
      }
    }
    base + graphStr + "\n}"
  }
}

object BaseViz {
  def apply[P <: Platform[P]](tail: Producer[P, _], writer: Writer):Unit = {
    writer.write(VizGraph(tail).toString)
  }
}
