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

package com.twitter.summingbird.storm


import com.twitter.summingbird.batch.{ BatchID, Batcher }
import backtype.storm.testing.{ CompleteTopologyParam, MockedSources }
import com.twitter.algebird.{MapAlgebra, Monoid}
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.tormenta.spout.Spout
import com.twitter.util.Future
import java.util.{Collections, HashMap, Map => JMap, UUID}
import java.util.concurrent.atomic.AtomicInteger
import org.specs._
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Properties
import scala.collection.JavaConverters._
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import com.twitter.summingbird.TestGraphs

import org.scalacheck._
import Gen._
import Arbitrary.arbitrary
import org.scalacheck.Prop._

import scala.collection.mutable.{Map => MMap}



object TopologyPlannerLaws extends Properties("StormDag") {
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  val testStore = MergeableStoreSupplier.from {MergeableStore.fromStore[(Int, BatchID), Int](new JMapStore[(Int, BatchID), Int]())}

  val genSource1 = value(Storm.source(TraversableSpout(List[Int]())))
  val genSource2 = value(IdentityKeyedProducer(Storm.source(TraversableSpout(List[(Int, Int)]()))))

  // Put the non-recursive calls first, otherwise you blow the stack
  lazy val genOptMap11 = for {
    fn <- arbitrary[(Int) => Option[Int]]
    in <- genProd1
  } yield OptionMappedProducer(in, fn, manifest[Int])

  lazy val genOptMap12 = for {
    fn <- arbitrary[(Int) => Option[(Int,Int)]]
    in <- genProd1
  } yield IdentityKeyedProducer(OptionMappedProducer(in, fn, manifest[(Int, Int)]))

  lazy val genMerged1 = for {
    _  <- Gen.choose(0,1)
    p1 <- genProd1
    p2 <- genProd1
  } yield MergedProducer(p1, p2)


  lazy val genMerged2 = for {
    _  <- Gen.choose(0,1) 
    p1 <- genProd2
    p2 <- genProd2
  } yield IdentityKeyedProducer(MergedProducer(p1, p2))

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
  lazy val summed : Gen[StormDag]= for {
    in <- genProd2 
  } yield StormToplogyBuilder(in.sumByKey(testStore))

  // Removed Summable from here, so we never should recurse
  def genProd2: Gen[KeyedProducer[Storm, Int, Int]] = frequency((15, genSource2), (5, genOptMap12), (5, genOptMap22), (1, genMerged2))
  def genProd1: Gen[Producer[Storm, Int]] = frequency((15, genSource1), (5, genOptMap11), (5, genOptMap21), (1, genMerged1))

  implicit def genProducer: Arbitrary[StormDag] = Arbitrary(summed)


  val testFn = { i: Int => List((i -> i)) }


  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def dumpFailingGraph(dag: StormDag) = {
    import java.io._
    import com.twitter.summingbird.storm.viz.StormViz
    val writer2 = new PrintWriter(new File("/tmp/failingGraph.dot"))
    StormViz(dag.tail, writer2)
    writer2.close()
  }

  property("Dag Nodes must be unique") = forAll { (dag: StormDag) =>
    dag.nodes.size == dag.nodes.toSet.size
  }

  property("Must have at least one producer in each StormNode") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      n.members.size > 0
    }
  }

  property("The any Summer must be the first in its node") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val firstP = n.members.last
      n.members.forall{p =>
        val inError = (p.isInstanceOf[Summer[_, _, _]] && p != firstP)
        if(inError) dumpFailingGraph(dag)
        !inError
      }
    }
  }

  property("The first producer in a storm node cannot be a NamedProducer") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val inError = n.members.last.isInstanceOf[NamedProducer[_, _]]
      if(inError) dumpFailingGraph(dag)
      !inError
    }
  }

  property("0 or more merge producers at the start of every storm bolts, followed by 1+ non-merge producers and no other merge producers following those.") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val (_, inError) = n.members.foldLeft((false, false)) { case ((seenMergeProducer, inError), producer) =>
        producer match {
          case MergedProducer(_, _) => (true, inError)
          case NamedProducer(_, _) => (seenMergeProducer, inError)
          case _ => (seenMergeProducer, (inError || seenMergeProducer))
        }
      }
      if(inError) dumpFailingGraph(dag)
      !inError
    }
  }

  property("The the last producer in any StormNode must be a KeyedProducer") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val firstP = n.members.last
      firstP match {
        case Summer(_, _, _) =>
            dag.dependsOn(n).forall {otherN =>
              otherN.members.head.isInstanceOf[KeyedProducer[_, _, _]]
            }
        case _ => true
      }
    }
  }

  property("No producer is repeated") = forAll { (dag: StormDag) =>
    val numAllProducers = dag.nodes.foldLeft(0){(sum, n) => sum + n.members.size}
    val allProducersSet = dag.nodes.foldLeft(Set[Producer[Storm, _]]()){(runningSet, n) => runningSet | n.members.toSet}
    numAllProducers == allProducersSet.size
  }
  

  property("All producers are in a StormNode") = forAll { (dag: StormDag) =>
    val allProducers = Producer.transitiveDependenciesOf(dag.tail).toSet + dag.tail
    val numAllProducersInDag = dag.nodes.foldLeft(0){(sum, n) => sum + n.members.size}
    allProducers.size == numAllProducersInDag
  }

  property("Only spouts can have no incoming dependencies") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      n match {
        case _: StormSpout => true
        case _ => dag.dependsOn(n).size > 0
      }
    }
  }


  property("Spouts must have no incoming dependencies, and they must have dependants") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      n match {
        case _: StormSpout => 
          dag.dependsOn(n).size == 0 && dag.dependantsOf(n).size > 0
        case _ => true
      }
    }
  }


  property("Prior to a summer the StormNode should be a FinalFlatMapStormBolt") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val firstP = n.members.last
      firstP match {
        case Summer(_, _, _) =>
            dag.dependsOn(n).forall {otherN =>
              otherN.isInstanceOf[FinalFlatMapStormBolt]
            }
        case _ => true
      }
    }
  }
  // -> Prior to a summer should be a final flatmap bolt, any bolt not prior to a summer should be a intermediate flatmap bolt

  // val nextFn = { pair: ((Int, (Int, Option[Int]))) =>
  //   val (k, (v, joinedV)) = pair
  //   List((k -> joinedV.getOrElse(10)))
  // }

  // def numFinalFlatMapStormBolt(d: StormDag): Int = d.nodes.foldLeft(0){ case (total, current) =>
  //     current match {
  //       case FinalFlatMapStormBolt(_) => total + 1
  //       case _ => total
  //     }
  // }

  // "Get Storm" in {
  //   val original = sample[List[Int]]
  //   val testSink = () => ((x: Any) => com.twitter.util.Future.Unit)
    

  //   val fn_1 = (x: Int) => List((x, x))
  //   val fn_2 = (x: Int) => List((x, x))

  //   val summer = TestGraphs.diamondJob[Storm, Int, Int, Int](Storm.source(TraversableSpout(original)), testSink, testStore)(fn_1)(fn_2)
  //   val dag: StormDag = genDagForTail(summer)

  //   numSummer(dag) == 1
  //   //must beTrue
  // }

  
}
