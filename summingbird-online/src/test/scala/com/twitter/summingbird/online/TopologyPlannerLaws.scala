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

package com.twitter.summingbird.online

import com.twitter.summingbird._
import com.twitter.summingbird.planner._
import com.twitter.summingbird.memory.Memory
import scala.collection.mutable.{Map => MMap}
import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._



object TopologyPlannerLaws extends Properties("Online Dag") {

  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  private type MemoryDag = Dag[Memory]

  import TestGraphGenerators._

  implicit def sink1: Memory#Sink[Int] = sample[Int => Unit]
  implicit def sink2: Memory#Sink[(Int, Int)] =  sample[((Int, Int)) => Unit]

  implicit def testStore: Memory#Store[Int, Int] = MMap[Int, Int]()

  implicit val arbSource1: Arbitrary[Producer[Memory, Int]] = 
          Arbitrary(Gen.listOfN(100, Arbitrary.arbitrary[Int]).map{
              x: List[Int] =>
                Memory.toSource(x)})
  implicit val arbSource2: Arbitrary[KeyedProducer[Memory, Int, Int]] = 
          Arbitrary(Gen.listOfN(100, Arbitrary.arbitrary[(Int, Int)]).map{
            x: List[(Int, Int)] => 
              IdentityKeyedProducer(Memory.toSource(x))})

  
  lazy val genDag : Gen[MemoryDag]= for {
    tail <- summed 
  } yield OnlinePlan(tail)

  implicit def genProducer: Arbitrary[MemoryDag] = Arbitrary(genDag)


  
  val testFn = { i: Int => List((i -> i)) }

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  var dumpNumber = 1
  def dumpGraph(dag: MemoryDag) = {
    import java.io._
    import com.twitter.summingbird.viz.VizGraph
    val writer2 = new PrintWriter(new File("/tmp/failingGraph" + dumpNumber + ".dot"))
    VizGraph(dag, writer2)
    writer2.close()
    dumpNumber = dumpNumber + 1
  }

  def isNoOpProducer(p: Producer[_, _]): Boolean = {
    p match {
      case IdentityKeyedProducer(_) => true
      case NamedProducer(_, _) => true
      case MergedProducer(_, _) => true
      case AlsoProducer(_, _) => true
      case _ => false
    }
  }

  property("Dag Nodes must be unique") = forAll { (dag: MemoryDag) =>
    dag.nodes.size == dag.nodes.toSet.size
  }

  property("Must have at least one producer in each MemoryNode") = forAll { (dag: MemoryDag) =>
    dag.nodes.forall{n =>
      n.members.size > 0
    }
  }

  property("If a MemoryNode contains a Summer, all other producers must be NOP's") = forAll { (dag: MemoryDag) =>
    dag.nodes.forall{n =>
      val producersWithoutNOP = n.members.filterNot(isNoOpProducer(_))
      val firstP = producersWithoutNOP.headOption
      producersWithoutNOP.forall{p =>
        val inError = (p.isInstanceOf[Summer[_, _, _]] && producersWithoutNOP.size != 1)
        if(inError) dumpGraph(dag)
        !inError
      }
    }
  }

  property("The first producer in a online node cannot be a NamedProducer") = forAll { (dag: MemoryDag) =>
    dag.nodes.forall{n =>
      val inError = n.members.last.isInstanceOf[NamedProducer[_, _]]
      if(inError) dumpGraph(dag)
      !inError
    }
  }

  property("0 or more merge producers at the start of every online bolts, followed by 1+ non-merge producers and no other merge producers following those.") = forAll { (dag: MemoryDag) =>
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

  property("No producer is repeated") = forAll { (dag: MemoryDag) =>
    val numAllProducers = dag.nodes.foldLeft(0){(sum, n) => sum + n.members.size}
    val allProducersSet = dag.nodes.foldLeft(Set[Producer[Memory, _]]()){(runningSet, n) => runningSet | n.members.toSet}
    numAllProducers == allProducersSet.size
  }
  

  property("All producers are in a Node") = forAll { (dag: MemoryDag) =>
    val allProducers = Producer.entireGraphOf(dag.tail).toSet + dag.tail
    val numAllProducersInDag = dag.nodes.foldLeft(0){(sum, n) => sum + n.members.size}
    allProducers.size == numAllProducersInDag
  }

  property("Only sources can have no incoming dependencies") = forAll { (dag: MemoryDag) =>
    dag.nodes.forall{n =>
      val success = n match {
        case _: SourceNode[_] => true
        case _ => dag.dependenciesOf(n).size > 0
      }
      if(!success) dumpGraph(dag)
      success 
    }
  }


  property("Sources must have no incoming dependencies, and they must have dependants") = forAll { (dag: MemoryDag) =>
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


  property("Prior to a summer the Nonde should be a FlatMap Node") = forAll { (dag: MemoryDag) =>
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

  property("There should be no flatmap producers in the source node") = forAll { (dag: MemoryDag) =>
    dag.nodes.forall{n =>
      val success = n match {
        case n: SourceNode[_] => n.members.forall{p => !p.isInstanceOf[FlatMappedProducer[_, _, _]]}
        case _ => true
      }
      if(!success) dumpGraph(dag)
      success
    }
  }

  property("Nodes in the DAG should have unique names") = forAll { (dag: MemoryDag) =>
    val allNames = dag.nodes.toList.map{n => dag.getNodeName(n)}
    allNames.size == allNames.distinct.size
  }

}
