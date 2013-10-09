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

import com.twitter.storehaus.JMapStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.planner._
import com.twitter.summingbird.storm.planner._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.util.Future
import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._



object TopologyPlannerLaws extends Properties("StormDag") {

  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit
  private type StormDag = Dag[Storm]

  import TestGraphGenerators._
  implicit def sink1: Storm#Sink[Int] = (() => ((_) => Future.Unit))
  implicit def sink2: Storm#Sink[(Int, Int)] = (() => ((_) => Future.Unit))

  implicit def testStore: Storm#Store[Int, Int] = MergeableStoreSupplier.from {MergeableStore.fromStore[(Int, BatchID), Int](new JMapStore[(Int, BatchID), Int]())}

  implicit def arbSource1: Arbitrary[Producer[Storm, Int]] = Arbitrary(Gen.listOfN(5000, Arbitrary.arbitrary[Int]).map{x: List[Int] =>  Storm.source(TraversableSpout(x))})
  implicit def arbSource2: Arbitrary[KeyedProducer[Storm, Int, Int]] = Arbitrary(Gen.listOfN(5000, Arbitrary.arbitrary[(Int, Int)]).map{x: List[(Int, Int)] => IdentityKeyedProducer(Storm.source(TraversableSpout(x)))})

  
  lazy val genDag : Gen[StormDag]= for {
    tail <- summed 
  } yield DagBuilder(tail)

  implicit def genProducer: Arbitrary[StormDag] = Arbitrary(genDag)


  
  val testFn = { i: Int => List((i -> i)) }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  var dumpNumber = 1
  def dumpGraph(dag: StormDag) = {
    import java.io._
    import com.twitter.summingbird.viz.VizGraph
    val writer2 = new PrintWriter(new File("/tmp/failingGraph" + dumpNumber + ".dot"))
    VizGraph(dag, writer2)
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

  property("The last producer in any StormNode prior to a summer must be a KeyedProducer") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val firstP = n.members.last
      firstP match {
        case Summer(_, _, _) =>
            dag.dependantsOf(n).forall {otherN =>
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
    val allProducers = Producer.entireGraphOf(dag.tail).toSet + dag.tail
    val numAllProducersInDag = dag.nodes.foldLeft(0){(sum, n) => sum + n.members.size}
    allProducers.size == numAllProducersInDag
  }

  property("Only spouts can have no incoming dependencies") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val success = n match {
        case _: SourceNode[_] => true
        case _ => dag.dependenciesOf(n).size > 0
      }
      if(!success) dumpGraph(dag)
      success 
    }
  }


  property("Spouts must have no incoming dependencies, and they must have dependants") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val success = n match {
        case _: SourceNode[_] => 
          dag.dependenciesOf(n).size == 0 && dag.dependantsOf(n).size > 0
        case _ => true
      }
      if(!success) dumpGraph(dag)
      success 
    }
  }


  property("Prior to a summer the StormNode should be a FinalFlatMapStormBolt") = forAll { (dag: StormDag) =>
    dag.nodes.forall{n =>
      val firstP = n.members.last
      val success = firstP match {
        case Summer(_, _, _) =>
            dag.dependenciesOf(n).size > 0 && dag.dependenciesOf(n).forall {otherN =>
              otherN.isInstanceOf[FlatMapNode[_]]
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
        case n: SourceNode[_] => n.members.forall{p => !p.isInstanceOf[FlatMappedProducer[_, _, _]]}
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

  property("Can plan to a Storm Topology") = forAll { (dag: StormDag) => 
    try {
      Storm.local().plan(dag.tail)
      true
      } catch {
        case e: Throwable =>
        dumpGraph(dag)
        println(e)
        e.printStackTrace
        false
      }

  }
}
