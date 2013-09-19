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
import Arbitrary.arbitrary
import org.scalacheck.Prop._

import scala.collection.mutable.{Map => MMap}

object DependantsTest extends Properties("Dependants") {

  val genSource1 = value(Producer.source[Memory, Int](List[Int]()))
  val genSource2 = value(IdentityKeyedProducer(Producer.source[Memory, (Int, Int)](List[(Int, Int)]())))

  // Put the non-recursive calls first, otherwise you blow the stack
  lazy val genOptMap11 = for {
    fn <- arbitrary[(Int) => Option[Int]]
    in <- genProd1
  } yield OptionMappedProducer(in, fn, manifest[Int])

  lazy val genOptMap12 = for {
    fn <- arbitrary[(Int) => Option[(Int,Int)]]
    in <- genProd1
  } yield IdentityKeyedProducer(OptionMappedProducer(in, fn, manifest[(Int, Int)]))

  lazy val genOptMap21 = for {
    fn <- arbitrary[((Int,Int)) => Option[Int]]
    in <- genProd2
  } yield OptionMappedProducer(in, fn, manifest[Int])

  lazy val genOptMap22 = for {
    fn <- arbitrary[((Int,Int)) => Option[(Int,Int)]]
    in <- genProd2
  } yield IdentityKeyedProducer(OptionMappedProducer(in, fn, manifest[(Int, Int)]))
  // TODO (https://github.com/twitter/summingbird/issues/74): add more
  // nodes, abstract over Platform
  lazy val summed = for {
    in <- genSummable // don't sum sums
  } yield in.sumByKey(MMap[Int, Int]())

  // We bias towards sources so the trees don't get too deep
  def genSummable: Gen[KeyedProducer[Memory, Int, Int]] = frequency((2, genSource2), (1, genOptMap12), (1, genOptMap22))
  def genProd2: Gen[KeyedProducer[Memory, Int, Int]] = frequency((3, genSource2), (1, genOptMap12), (1, genOptMap22), (1, summed))
  def genProd1: Gen[Producer[Memory, Int]] = frequency((3, genSource1), (1, genOptMap11), (1, genOptMap21))

  implicit def genProducer: Arbitrary[Producer[Memory, _]] = Arbitrary(oneOf(genProd1, genProd2))

  property("transitive deps includes non-transitive") = forAll { (prod: Producer[Memory, _]) =>
    val deps = Producer.dependenciesOf(prod).toSet
    (Producer.transitiveDependenciesOf(prod).toSet & deps) == deps
  }
  property("we don't depend on ourself") = forAll { (prod: Producer[Memory, _]) =>
    !((Producer.dependenciesOf(prod) ++ Producer.transitiveDependenciesOf(prod)).toSet.contains(prod))
  }
  property("if transitive deps == non-transitive, then parents are sources") = forAll { (prod: Producer[Memory, _]) =>
    val deps = Producer.dependenciesOf(prod)
    (Producer.transitiveDependenciesOf(prod) == deps) ==> (deps.forall { case s@Source(_, _) => true; case _ => false })
  }
  def implies(a: Boolean, b: => Boolean): Boolean = if (a) b else true

  property("Sources all the only things of depth == 0") = forAll { (prod: Producer[Memory, _]) =>
    val deps = Dependants(prod)
    Producer.transitiveDependenciesOf(prod).forall { t =>
      val tdepth = deps.depth(t).get
      implies(tdepth == 0, t.isInstanceOf[Source[_, _]]) &&
      implies(tdepth > 0, (Producer.dependenciesOf(t).map { deps.depth(_).get }.max) < tdepth) &&
      implies(tdepth > 0, Producer.dependenciesOf(t).exists { deps.depth(_) == Some(tdepth-1) })
    }
  }

  property("Tails have max depth") = forAll { (tail: Producer[Memory, _]) =>
    val deps = Dependants(tail)
    Producer.transitiveDependenciesOf(tail).forall { t =>
      deps.depth(t).get < deps.depth(tail).get
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

  property("if A is a dependency of B, then B is a dependant of A") = forAll { (prod:  Producer[Memory, _]) =>
    val dependants = Dependants(prod)
    Producer.transitiveDependenciesOf(prod).forall { child =>
      Producer.dependenciesOf(child).forall { parent => dependants.dependantsOf(parent).get.contains(child) } &&
        (dependants.fanOut(child).get > 0)
    } && (dependants.fanOut(prod) == Some(0))
  }
}
