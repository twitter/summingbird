package com.twitter.summingbird.planner

import com.twitter.summingbird._
import com.twitter.summingbird.graph.Rule
import com.twitter.summingbird.memory._

import org.scalatest.FunSuite
import org.scalacheck.{Arbitrary, Gen}
import Gen.oneOf

import scala.collection.mutable
import org.scalatest.prop.GeneratorDrivenPropertyChecks._

class DagOptimizerTest extends FunSuite {

  implicit val generatorDrivenConfig =
    PropertyCheckConfig(minSuccessful = 10000)

  import TestGraphGenerators._
  import MemoryArbitraries._
  implicit def testStore: Memory#Store[Int, Int] = mutable.Map[Int, Int]()
  implicit def testService: Memory#Service[Int, Int] = new mutable.HashMap[Int, Int]() with MemoryService[Int, Int]
  implicit def sink1: Memory#Sink[Int] = ((_) => Unit)
  implicit def sink2: Memory#Sink[(Int, Int)] = ((_) => Unit)

  def genProducer: Gen[Producer[Memory, _]] = oneOf(genProd1, genProd2, summed)

  test("DagOptimizer round trips") {
    forAll { p: Producer[Memory, Int] =>
      val dagOpt = new DagOptimizer[Memory] { }

      assert(dagOpt.toLiteral(p).evaluate == p)
    }
  }

  test("ExpressionDag fanOut matches DependantGraph") {
    forAll(genProducer) { p: Producer[Memory, _] =>
      val dagOpt = new DagOptimizer[Memory] { }

      val expDag = dagOpt.expressionDag(p)._1

      val deps = Dependants(p)

      deps.nodes.foreach { n =>
        deps.fanOut(n) match {
          case Some(fo) => assert(expDag.fanOut(n) == fo)
          case None => fail(s"node $n has no fanOut value")
        }
      }
    }
  }

  test("Rules are idempotent") {
    val dagOpt = new DagOptimizer[Memory] { }
    import dagOpt._

    val allRules = List(RemoveNames,
      RemoveIdentityKeyed,
      FlatMapFusion,
      OptionMapFusion,
      OptionToFlatMap,
      KeyFlatMapToFlatMap,
      FlatMapKeyFusion,
      ValueFlatMapToFlatMap,
      FlatMapValuesFusion,
      FlatThenOptionFusion,
      DiamondToFlatMap,
      MergePullUp,
      AlsoPullUp)

    val genRule: Gen[Rule[Prod]] =
      for {
        n <- Gen.choose(1, allRules.size)
        rs <- Gen.pick(n, allRules) // get n randomly selected
      } yield rs.reduce(_.orElse(_))

    forAll(genProducer, genRule) { (p, r) =>
      val once = optimize(p, r)
      val twice = optimize(once, r)
      assert(once == twice)
    }

  }
}
