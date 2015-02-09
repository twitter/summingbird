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
  def apply[P <: Platform[P], T](tail: TailProducer[P, T]): (Map[Producer[P, Any], List[String]], TailProducer[P, T]) = {
    val opt = new DagOptimizer[P] {}
    val newTail = opt.optimize(tail, opt.RemoveNames).asInstanceOf[TailProducer[P, T]]
    val dependants = Dependants(tail)
    // Where is each irreducible in the original graph:
    val irrMap: Map[Irreducible[P], List[Producer[P, Any]]] = (for {
      prod <- dependants.nodes
      irr <- Producer.irreduciblesOf(prod)
    } yield irr -> prod)
      .groupBy(_._1)
      .map { case (irr, it) => irr -> it.map(_._2).toList }

    def originalName(p: Producer[P, Any]): List[String] =
      dependants.namesOf(p).map(_.id)

    def newToOld(p: Producer[P, Any]): List[Producer[P, Any]] =
      Producer.irreduciblesOf(p).flatMap(irrMap)

    def newName(p: Producer[P, Any]): List[String] =
      newToOld(p).flatMap(originalName)

    val newDependants = Dependants(newTail)

    (newDependants.nodes.map { n => n -> newName(n) }.toMap, newTail)
  }
}
