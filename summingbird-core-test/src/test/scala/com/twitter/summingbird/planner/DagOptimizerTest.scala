package com.twitter.summingbird.planner

import com.twitter.summingbird._
import com.twitter.summingbird.graph.{DependantGraph, Rule}
import com.twitter.summingbird.memory._

import org.scalatest.FunSuite
import org.scalacheck.{Arbitrary, Gen}
import Gen.oneOf

import scala.collection.mutable
import org.scalatest.prop.GeneratorDrivenPropertyChecks._

class DagOptimizerTest extends FunSuite {

  implicit val generatorDrivenConfig =
    PropertyCheckConfig(minSuccessful = 10000, maxDiscarded = 1000) // the producer generator uses filter, I think

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

  val dagOpt = new DagOptimizer[Memory] { }

  test("ExpressionDag fanOut matches DependantGraph") {
    forAll(genProducer) { p: Producer[Memory, _] =>
      val expDag = dagOpt.expressionDag(p)._1

      // the expression considers an also a fanout, so
      // we can't use the standard Dependants, we need to
      // us parentsOf as the edge function
      val deps = new DependantGraph[Producer[Memory, Any]] {
        override lazy val nodes: List[Producer[Memory, Any]] = Producer.entireGraphOf(p)
        override def dependenciesOf(p: Producer[Memory, Any]) = Producer.parentsOf(p)
      }

      deps.nodes.foreach { n =>
        deps.fanOut(n) match {
          case Some(fo) => assert(expDag.fanOut(n) == fo)
          case None => fail(s"node $n has no fanOut value")
        }
      }
    }
  }


  val allRules = {
    import dagOpt._

    List(RemoveNames,
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
  }

  val genRule: Gen[Rule[dagOpt.Prod]] =
    for {
      n <- Gen.choose(1, allRules.size)
      rs <- Gen.pick(n, allRules) // get n randomly selected
    } yield rs.reduce(_.orElse(_))

  test("Rules are idempotent") {
    forAll(genProducer, genRule) { (p, r) =>
      val once = dagOpt.optimize(p, r)
      val twice = dagOpt.optimize(once, r)
      assert(once == twice)
    }
  }

  test("fanOut matches after optimization") {

    forAll(genProducer, genRule) { (p, r) =>

      val once = dagOpt.optimize(p, r)

      val expDag = dagOpt.expressionDag(once)._1
      // the expression considers an also a fanout, so
      // we can't use the standard Dependants, we need to
      // us parentsOf as the edge function
      val deps = new DependantGraph[Producer[Memory, Any]] {
        override lazy val nodes: List[Producer[Memory, Any]] = Producer.entireGraphOf(once)
        override def dependenciesOf(p: Producer[Memory, Any]) = Producer.parentsOf(p)
      }

      deps.nodes.foreach { n =>
        deps.fanOut(n) match {
          case Some(fo) => assert(expDag.fanOut(n) == fo, s"node: $n, in optimized: $once")
          case None => fail(s"node $n has no fanOut value")
        }
      }
    }

  }
}
