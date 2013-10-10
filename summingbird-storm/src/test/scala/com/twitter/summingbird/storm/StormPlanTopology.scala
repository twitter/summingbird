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
import com.twitter.storehaus.{ ReadableStore, JMapStore }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.util.Future
import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._



object StormPlanTopology extends Properties("StormDag") {

  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit
  private type StormDag = Dag[Storm]

  import TestGraphGenerators._
  implicit def sink1: Storm#Sink[Int] = (() => ((_) => Future.Unit))
  implicit def sink2: Storm#Sink[(Int, Int)] = (() => ((_) => Future.Unit))

  implicit def testStore: Storm#Store[Int, Int] = MergeableStoreSupplier.from {MergeableStore.fromStore[(Int, BatchID), Int](new JMapStore[(Int, BatchID), Int]())}

  implicit def arbSource1: Arbitrary[Producer[Storm, Int]] = Arbitrary(Gen.listOfN(5000, Arbitrary.arbitrary[Int]).map{x: List[Int] =>  Storm.source(TraversableSpout(x))})
  implicit def arbSource2: Arbitrary[KeyedProducer[Storm, Int, Int]] = Arbitrary(Gen.listOfN(5000, Arbitrary.arbitrary[(Int, Int)]).map{x: List[(Int, Int)] => IdentityKeyedProducer(Storm.source(TraversableSpout(x)))})

  implicit def arbService2: Arbitrary[Storm#Service[Int, Int]] = Arbitrary(Arbitrary.arbitrary[Int => Option[Int]].map{fn => StoreWrapper[Int, Int](() => ReadableStore.fromFn(fn))})
  
  lazy val genDag : Gen[StormDag]= for {
    tail <- summed 
  } yield OnlinePlan(tail)

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
