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

package com.twitter.summingbird

import com.twitter.summingbird.memory._

import org.scalacheck._
import Gen._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import scala.util.Random

import scala.collection.mutable.{ Map => MMap, HashMap => MHashMap }

object DependantsTest extends Properties("Dependants") {
  import TestGraphGenerators._
  import MemoryArbitraries._
  implicit def testStore: Memory#Store[Int, Int] = MMap[Int, Int]()
  implicit def testService: Memory#Service[Int, Int] = new MHashMap[Int, Int]() with MemoryService[Int, Int]
  implicit def sink1: Memory#Sink[Int] = ((_) => Unit)
  implicit def sink2: Memory#Sink[(Int, Int)] = ((_) => Unit)

  implicit def genProducer: Arbitrary[Producer[Memory, _]] = Arbitrary(oneOf(genProd1, genProd2, summed))

  property("transitive deps includes non-transitive") = forAll { (prod: Producer[Memory, _]) =>
    val deps = Producer.dependenciesOf(prod).toSet
    (Producer.transitiveDependenciesOf(prod).toSet & deps) == deps
  }
  property("we don't depend on ourself") = forAll { (prod: Producer[Memory, _]) =>
    !((Producer.dependenciesOf(prod) ++ Producer.transitiveDependenciesOf(prod)).toSet.contains(prod))
  }

  property("if transitive deps == non-transitive, then parents are sources") = forAll { (prod: Producer[Memory, _]) =>
    val deps = Producer.dependenciesOf(prod)
    (Producer.transitiveDependenciesOf(prod) == deps) ==> {
      deps.forall { case s @ Source(_) => true; case _ => false }
    }
  }
  def implies(a: Boolean, b: => Boolean): Boolean = if (a) b else true

  property("Sources all the only things of depth == 0") = forAll { (prod: Producer[Memory, _]) =>
    val deps = Dependants(prod)
    deps.nodes.forall { t =>
      val tdepth = deps.depth(t).get
      implies(tdepth == 0, t.isInstanceOf[Source[_, _]]) &&
        implies(tdepth > 0, (Producer.dependenciesOf(t).map { deps.depth(_).get }.max) < tdepth) &&
        implies(tdepth > 0, Producer.dependenciesOf(t).exists { deps.depth(_) == Some(tdepth - 1) })
    }
  }

  property("Tails have max depth") = forAll { (tail: Producer[Memory, _]) =>
    val deps = Dependants(tail)
    deps.allTails.forall { thisTail =>
      Producer.transitiveDependenciesOf(thisTail).forall { t =>
        deps.depth(t).get < deps.depth(thisTail).get
      }
    }
  }

  property("The transitive dependencies list is unique") = forAll { (tail: Producer[Memory, _]) =>
    val deps = Producer.transitiveDependenciesOf(tail)
    deps.size == deps.toSet.size
  }

  property("The dependencies list is unique") = forAll { (tail: Producer[Memory, _]) =>
    val deps = Producer.dependenciesOf(tail)
    deps.size == deps.toSet.size
  }

  property("if A is a dependency of B, then B is a dependant of A") = forAll { (prod: Producer[Memory, _]) =>
    val dependants = Dependants(prod)

    dependants.nodes.forall { n =>
      Producer.dependenciesOf(n).forall { parent =>
        dependants.dependantsOf(parent).get.contains(n)
      }
    }
  }

  property("tails have no dependencies, and nodes with no dependencies are tails") =
    forAll { (prod: Producer[Memory, _]) =>
      val dependants = Dependants(prod)
      import dependants._

      val tails = allTails.toSet
      tails.map { dependantsOf(_) }.forall { _.get.isEmpty } && {
        nodes
          .filter { dependantsOf(_) == Some(Nil) }
          .forall(tails)
      }
    }

  property("finding all nodes and tails works") = forAll { (prod: Producer[Memory, _]) =>
    // Only also breaks the normal dependency rules to find all nodes
    // sorry for the cast, remove when the Producer variance is fixed
    def allParents(n: Producer[Memory, Any]): Set[Producer[Memory, Any]] = {
      n match {
        case AlsoProducer(l, r) =>
          Set(n, l, r).asInstanceOf[Set[Producer[Memory, Any]]]
        case _ =>
          (n :: Producer.dependenciesOf(n))
            .toSet
            .asInstanceOf[Set[Producer[Memory, Any]]]
      }
    }
    @annotation.tailrec
    def fix[T](acc: Set[T])(fn: T => Set[T]): Set[T] = {
      val newSet = acc.flatMap(fn)
      if (newSet == acc) acc
      else fix(newSet)(fn)
    }
    val alln = fix(Set(prod.asInstanceOf[Producer[Memory, Any]]))(allParents _)
    val deps = Dependants(prod)
    val tails = alln.filter(deps.fanOut(_).get == 0)
    (tails == deps.allTails.toSet) &&
      (tails.size == deps.allTails.size) &&
      (deps.nodes.toSet == alln) &&
      (deps.nodes.size == alln.size)
  }

  property("tails <= AlsoProducer count + 1") = forAll { (prod: Producer[Memory, _]) =>
    val dependants = Dependants(prod)

    (!dependants.allTails.isEmpty) && {
      val alsoCount = dependants.nodes.collect { case AlsoProducer(_, _) => 1 }.sum
      (dependants.allTails.size <= (alsoCount + 1))
    }
  }

  property("Sources + transitive dependants are all the nodes") = forAll { (prod: Producer[Memory, _]) =>
    val allNodes = Producer.entireGraphOf(prod)
    val sources = allNodes.collect { case s @ Source(_) => s }.toSet
    val dependants = Dependants(prod)
    val sAndDown = (sources ++ sources.flatMap { dependants.transitiveDependantsOf(_) })
    allNodes.toSet == sAndDown
  }

  property("transitiveDependantsTillOutput finds outputs as a subset of dependants") =
    forAll { (prod: Producer[Memory, _]) =>
      val dependants = Dependants(prod)
      dependants.nodes.forall { n =>
        val output = dependants.transitiveDependantsTillOutput(n).collect {
          case t: TailProducer[_, _] => t
        }.toSet[Producer[Memory, Any]]

        (dependants.transitiveDependantsOf(n).toSet intersect output) == output
      }
    }

  property("transitiveDependantsTillOutput is a subset of writers dependencies") =
    forAll { (prod: Producer[Memory, _]) =>
      val dependants = Dependants(prod)
      dependants.nodes.forall { n =>
        val depTillWrite = dependants.transitiveDependantsTillOutput(n)
        val writerDependencies = depTillWrite.collect {
          case t: TailProducer[_, _] => t
        }.flatMap { n => n :: Producer.transitiveDependenciesOf(n) }.toSet

        depTillWrite.collectFirst { case MergedProducer(_, _) => true }.getOrElse(false) || writerDependencies.isEmpty || ((depTillWrite.toSet intersect writerDependencies) == depTillWrite.toSet)
      }
    }

  property("transitiveDependantsTillOutput finds no children of outputs") = forAll { (prod: Producer[Memory, _]) =>
    val dependants = Dependants(prod)
    dependants.nodes.forall { n =>
      val tillWrite = dependants.transitiveDependantsTillOutput(n)
      val outputChildren = tillWrite.collect {
        case s @ Summer(_, _, _) => s
        case w @ WrittenProducer(_, _) => w
      }.flatMap { dependants.transitiveDependantsOf(_) }.toSet[Producer[Memory, Any]]
      tillWrite.collectFirst { case MergedProducer(_, _) => true }.getOrElse(false) || (tillWrite.toSet & outputChildren.toSet).size == 0
    }
  }

}
