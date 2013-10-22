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

package com.twitter.summingbird.planner

import com.twitter.summingbird._


object StripNamedNode {
  def castTail[P <: Platform[P]](node: Producer[P, Any]): TailProducer[P, Any] = node.asInstanceOf[TailProducer[P, Any]]
  def castToPair[P <: Platform[P]](node: Producer[P, Any]): Producer[P, (Any, Any)] = node.asInstanceOf[Producer[P, (Any, Any)]]
  def castToKeyed[P <: Platform[P]](node: Producer[P, Any]): KeyedProducer[P, Any, Any] = node.asInstanceOf[KeyedProducer[P, Any, Any]]
  def stripNamedNodes[P <: Platform[P]](map: Map[Producer[P, Any], Producer[P, Any]], node: Producer[P, Any]): (Map[Producer[P, Any], Producer[P, Any]], Producer[P, Any]) = {

    node match {
      case p@AlsoProducer(oL, oR) =>
          val (newMap, l) = stripNamedNodes(map, oL)
          val (nnewMap, r) = stripNamedNodes(newMap, oR)
          val added = AlsoProducer(castTail(l), r)
          (nnewMap + (added -> node), added)

      case p@NamedProducer(producer, _) => stripNamedNodes(map, producer)

      case p@Source(_) => (map + (p -> node), p)

      case p@IdentityKeyedProducer(producer) =>
          val (newMap, newP) = stripNamedNodes(map, producer)
          val added = p.copy(producer=castToPair(newP))
          (newMap + (added -> node), added)

      case p@OptionMappedProducer(producer, _) =>
          val (newMap, newP) = stripNamedNodes(map, producer)
          val added = p.copy(producer=castToPair(newP))
          (newMap + (added -> node), added)

      case p@FlatMappedProducer(producer, _) =>
          val (newMap, newP) = stripNamedNodes(map, producer)
          val added = p.copy(producer=castToPair(newP))
          (newMap + (added -> node), added)

      case p@KeyFlatMappedProducer(producer, _) =>
          val (newMap, newP) = stripNamedNodes(map, producer)
          val added = p.copy(producer=castToKeyed(newP))
          (newMap + (added -> p), added)

      case p@MergedProducer(oL, oR) =>
          val (newMap, l) = stripNamedNodes(map, oL)
          val (nnewMap, r) = stripNamedNodes(newMap, oR)
          val added = MergedProducer(l, r)
          (nnewMap + (added -> node), added)

      case p@LeftJoinedProducer(producer, _) =>
          val (newMap, newP) = stripNamedNodes(map, producer)
          val added = p.copy(left=castToKeyed(newP))
          (newMap + (added -> node), added)

      case p@Summer(producer, _, _) =>
          val (newMap, newP) = stripNamedNodes(map, producer)
          val added = p.copy(producer=castToKeyed(newP))
          (newMap + (added -> node), added)

      case p@WrittenProducer(producer, _) =>
        val (newMap, newP) = stripNamedNodes(map, producer)
          val added = p.copy(producer=castToPair(newP))
          (newMap + (added -> node), added)
    }
  }

  // Priority list of of names for a given producer
  private def getName[P <: Platform[P]](dependants: Dependants[P], producer: Producer[P, Any]): List[String] = {
    dependants.transitiveDependantsOf(producer).collect{case NamedProducer(_, n) => n}
  }


  def apply[P <: Platform[P], T](tail: TailProducer[P, T]): (Map[Producer[P, Any], List[String]], TailProducer[P, T]) = {
    val dependants = Dependants(tail)

    val (map, newTail) = stripNamedNodes(Map[Producer[P, Any], Producer[P, Any]](), tail)
    (map.mapValues(n => getName(dependants, n)), newTail.asInstanceOf[TailProducer[P, T]])
  }
}
