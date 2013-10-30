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


case class ProducerF[P <: Platform[P]](oldSources: List[Producer[P, Any]],
                          oldRef: Producer[P, Any],
                          f: List[Producer[P, Any]] => Producer[P, Any])

object StripNamedNode {

  def castTail[P <: Platform[P]](node: Producer[P, Any]): TailProducer[P, Any] = node.asInstanceOf[TailProducer[P, Any]]
  def castToPair[P <: Platform[P]](node: Producer[P, Any]): Producer[P, (Any, Any)] = node.asInstanceOf[Producer[P, (Any, Any)]]
  def castToKeyed[P <: Platform[P]](node: Producer[P, Any]): KeyedProducer[P, Any, Any] = node.asInstanceOf[KeyedProducer[P, Any, Any]]

  def processLevel[P <: Platform[P]](optLast: Option[Producer[P, Any]],
                  l: List[ProducerF[P]],
                  m: Map[Producer[P, Any], Producer[P, Any]],
                  op: PartialFunction[Producer[P, Any], Option[Producer[P, Any]]]): (Option[Producer[P, Any]], Map[Producer[P, Any], Producer[P, Any]]) = {
    l.foldLeft((optLast, m)){ case ((nOptLast, nm), pp) =>
      val ns = pp.oldSources.map(m(_))
      val res = pp.f(ns)
      val mutatedRes = if(op.isDefinedAt(res)) {
        op(res) match {
          case Some(p) => p
          case None => ns(0)
        }
      } else {
        res
      }

      (Some(mutatedRes), (nm + (pp.oldRef -> mutatedRes)))
    }
  }

  def functionize[P <: Platform[P]](node: Producer[P, Any]): ProducerF[P] = {
    node match {
      case p@AlsoProducer(_, _) => ProducerF(
                List(p.result, p.ensure),
                p,
                {(newEntries): List[Producer[P, Any]] => p.copy(ensure=castTail(newEntries(1)), result=newEntries(0))}
                )
      case p@NamedProducer(producer, _) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(producer=producerL(0))}
                )

      case p@Source(_) => ProducerF(
                List(),
                p,
                {producerL: List[Producer[P, Any]] => p}
                )

      case p@IdentityKeyedProducer(producer) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(producer=castToPair(producerL(0)))}
                )

      case p@OptionMappedProducer(producer, _) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(producer=producerL(0))}
                )

      case p@FlatMappedProducer(producer, _) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(producer=producerL(0))}
                )

      case p@KeyFlatMappedProducer(producer, _) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(producer=castToKeyed(producerL(0)))}
                )

      case p@MergedProducer(oL, oR) => ProducerF(
                List(oL, oR),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(left=producerL(0), right=producerL(1))}
                )

      case p@LeftJoinedProducer(producer, _) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(left=castToKeyed(producerL(0)))}
                )

      case p@Summer(producer, _, _) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(producer=castToKeyed(producerL(0)))}
                )

      case p@WrittenProducer(producer, _) => ProducerF(
                List(producer),
                p,
                {producerL: List[Producer[P, Any]] => p.copy(producer=producerL(0))}
                )
    }
  }

  def toFunctional[P <: Platform[P]](tail: Producer[P, Any]) = {
    val depthInfo = graph.dagDepth(Producer.entireGraphOf(tail))(Producer.parentsOf(_))

    val reversedGraph = depthInfo.toList.groupBy(_._2).mapValues(_.map(_._1))
    reversedGraph.mapValues(_.map(functionize(_)))
  }

  def mutateGraph[P <: Platform[P]](tail: Producer[P, Any], op: PartialFunction[Producer[P, Any], Option[Producer[P, Any]]]) = {
    val newT: Option[Producer[P, Any]] = None
    val x = toFunctional(tail).toList.sortBy(_._1)
    x.map(_._2).foldLeft((newT, Map[Producer[P, Any], Producer[P, Any]]())) { case ((optLast, curMap), v) =>
      processLevel(optLast, v, curMap, op)
    }
  }

  def stripNamedNodes[P <: Platform[P]](node: Producer[P, Any]): (Map[Producer[P, Any], Producer[P, Any]], Producer[P, Any]) = {
    def removeNamed: PartialFunction[Producer[P, Any], Option[Producer[P, Any]]] =
      { case p@NamedProducer(p2, _) => None }
    val (optTail, oldNewMap) = mutateGraph(node, removeNamed)
    val newTail = optTail.get
    (oldNewMap.map(x => (x._2, x._1)).toMap, optTail.get)
  }

  // Priority list of of names for a given producer
  private def getName[P <: Platform[P]](dependants: Dependants[P], producer: Producer[P, Any]): List[String] = {
    (producer :: dependants.transitiveDependantsOf(producer)).collect{case NamedProducer(_, n) => n}
  }

  def apply[P <: Platform[P], T](tail: TailProducer[P, T]): (Map[Producer[P, Any], List[String]], TailProducer[P, T]) = {
    val dependants = Dependants(tail)
    val (map, newTail) = stripNamedNodes(tail)
    (map.mapValues(n => getName(dependants, n)), newTail.asInstanceOf[TailProducer[P, T]])
  }
}
