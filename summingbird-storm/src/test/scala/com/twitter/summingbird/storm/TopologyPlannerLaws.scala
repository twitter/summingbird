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

  import TestGraphGenerators._
  implicit def sink1: Storm#Sink[Int] = (() => ((_) => Future.Unit))
  implicit def sink2: Storm#Sink[(Int, Int)] = (() => ((_) => Future.Unit))

  implicit def testStore: Storm#Store[Int, Int] = MergeableStoreSupplier.from {MergeableStore.fromStore[(Int, BatchID), Int](new JMapStore[(Int, BatchID), Int]())}
  def buildSource() = {
    println("Creating source1")
    
  }
  implicit def genSource1: Gen[Producer[Storm, Int]] = Gen.choose(1,100000000).map(x => Storm.source(TraversableSpout(List[Int]())))
  implicit def genSource2: Gen[KeyedProducer[Storm, Int, Int]] = Gen.choose(1,100000000).map(x => IdentityKeyedProducer(Storm.source(TraversableSpout(List[(Int, Int)]()))))
  
  lazy val genDag : Gen[StormDag]= for {
    tail <- summed 
  } yield DagBuilder(tail)

  implicit def genProducer: Arbitrary[StormDag] = Arbitrary(genDag)


  
  val testFn = { i: Int => List((i -> i)) }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  var dumpNumber = 1
  def dumpGraph(dag: StormDag) = {
    import java.io._
    import com.twitter.summingbird.storm.viz.VizGraph
    val writer2 = new PrintWriter(new File("/tmp/failingGraph" + dumpNumber + ".dot"))
    VizGraph(dag.tail, writer2)
    writer2.close()
    dumpNumber = dumpNumber + 1
  }

  property("Dag Nodes must be unique") = forAll { (dag: StormDag) =>
    dag.nodes.size == dag.nodes.toSet.size
  }

  property("Must have at least one producer in each StormNode") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      n.members.size > 0
    }
  }

  property("If a StormNode contains a Summer, it must be the first Producer in that StormNode") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val firstP = n.members.last
      n.members.forall{p =>
        val inError = (p.isInstanceOf[Summer[_, _, _]] && p != firstP)
        if(inError) dumpGraph(dag)
        !inError
      }
    }
  }

  property("The first producer in a storm node cannot be a NamedProducer") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val inError = n.members.last.isInstanceOf[NamedProducer[_, _]]
      if(inError) dumpGraph(dag)
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
      if(inError) dumpGraph(dag)
      !inError
    }
  }

  property("The the last producer in any StormNode prior to a summer must be a KeyedProducer") = forAll { (dag: StormDag) =>
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
        case _: SourceNode => true
        case _ => dag.dependsOn(n).size > 0
      }
    }
  }


  property("Spouts must have no incoming dependencies, and they must have dependants") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      n match {
        case _: SourceNode => 
          dag.dependsOn(n).size == 0 && dag.dependantsOf(n).size > 0
        case _ => true
      }
    }
  }


  property("Prior to a summer the StormNode should be a FinalFlatMapStormBolt") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val firstP = n.members.last
      val success = firstP match {
        case Summer(_, _, _) =>
            dag.dependsOn(n).size > 0 && dag.dependsOn(n).forall {otherN =>
              otherN.isInstanceOf[FlatMapNode]
            }
        case _ => true
      }
      if(!success) dumpGraph(dag)
      success 
    }
  }

  property("There should be no flatmap producers in the source node") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val success = n match {
        case n: SourceNode => n.members.forall{p => !p.isInstanceOf[FlatMappedProducer[_, _, _]]}
        case _ => true
      }
      if(!success) dumpGraph(dag)
      success
    }
  }

  property("Nodes in the storm DAG should have unique names") = forAll { (dag: StormDag) =>
    val allNames = dag.nodes.toList.map{n => dag.getNodeName(n)}
    allNames.size == allNames.distinct.size
  }
}
