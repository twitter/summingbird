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

  @annotation.tailrec
  private def recurseGetNode(n :Producer[P, _], name: String): (String, Set[Producer[P, _]])  = {
    val children: Set[Producer[P, _]] = dependantState.dependantsOf(n).getOrElse(Set[Producer[P, _]]())
    children.headOption match {
      case Some(child: NamedProducer[_, _]) =>
        recurseGetNode(child, child.id)
      case Some(child: IdentityKeyedProducer[_, _, _]) =>
        recurseGetNode(child, name)
      case _ =>
        (name, children)
    }
  }


  override def toString() : String = {
    val base = "graph summingbirdGraph {\n"
    val graphStr = dependantState.nodes.foldLeft("") {(runningStr, nextNode) =>
      nextNode match {
        case NamedProducer(parent, name) => runningStr
        case i : IdentityKeyedProducer[_, _, _] => runningStr
        case _ => {
          val (nodeName, children) = recurseGetNode(nextNode, nextNode.getClass.toString)
          runningStr + children.foldLeft("") {(r, c) =>
            val (childName, _) = recurseGetNode(c, c.getClass.toString)
            r + "\"" + nodeName + "\" -- \"" + childName + "\"\n"
          }
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
