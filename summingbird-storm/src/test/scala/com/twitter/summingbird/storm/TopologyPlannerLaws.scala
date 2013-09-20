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
  } yield in.sumByKey(testStore)

  // We bias towards sources so the trees don't get too deep
  def genSummable: Gen[KeyedProducer[Storm, Int, Int]] = frequency((2, genSource2), (1, genOptMap12), (1, genOptMap22))
  def genProd2: Gen[KeyedProducer[Storm, Int, Int]] = frequency((3, genSource2), (1, genOptMap12), (1, genOptMap22), (1, summed))
  def genProd1: Gen[Producer[Storm, Int]] = frequency((3, genSource1), (1, genOptMap11), (1, genOptMap21))

  implicit def genProducer: Arbitrary[Producer[Storm, _]] = Arbitrary(oneOf(genProd1, genProd2))


  val testFn = { i: Int => List((i -> i)) }


  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  def genDagForTail[T](tail: Producer[Storm, T]): StormDag = {
    val dep = Dependants(tail)
    val fanOutSet =
      Producer.transitiveDependenciesOf(tail)
        .filter(dep.fanOut(_).exists(_ > 1)).toSet

    val topoBuilder = new StormToplogyBuilder(tail)
    val (stormRegistry, _) = topoBuilder.collectPass(tail, IntermediateFlatMapStormBolt(), StormRegistry(), fanOutSet, Set())
    StormDag.build(stormRegistry)
  }


  property("There should only be one and only one summer") = forAll { (tail: Producer[Storm, _]) =>
    val d = genDagForTail(tail)
    val numSummers = d.nodes.foldLeft(0){ case (total, current) =>
      current match {
        case SummerStormBolt(_) => total + 1
        case _ => total
      }
    }
    numSummers == 1
  }

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
