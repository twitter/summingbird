package com.twitter.summingbird.planner

import com.twitter.summingbird._
import com.twitter.summingbird.memory._

import org.scalatest.FunSuite
import org.scalacheck.{Arbitrary, Gen}
import Gen.oneOf

import scala.collection.mutable
import org.scalatest.prop.GeneratorDrivenPropertyChecks._

class DagOptimizerTest extends FunSuite {

  import TestGraphGenerators._
  import MemoryArbitraries._
  implicit def testStore: Memory#Store[Int, Int] = mutable.Map[Int, Int]()
  implicit def testService: Memory#Service[Int, Int] = new mutable.HashMap[Int, Int]() with MemoryService[Int, Int]
  implicit def sink1: Memory#Sink[Int] = ((_) => Unit)
  implicit def sink2: Memory#Sink[(Int, Int)] = ((_) => Unit)

  implicit def genProducer: Arbitrary[Producer[Memory, _]] = Arbitrary(oneOf(genProd1, genProd2, summed))

  test("DagOptimizer round trips") {
    forAll { p: Producer[Memory, Int] =>
      val dagOpt = new DagOptimizer[Memory] { }

      assert(dagOpt.toLiteral(p).evaluate == p)
    }
  }
}
