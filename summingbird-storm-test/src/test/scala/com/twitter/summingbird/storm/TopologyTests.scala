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

import org.apache.storm.generated.StormTopology
import com.twitter.algebird.MapAlgebra
import com.twitter.summingbird._
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.storm.spout.TraversableSpout
import org.scalatest.WordSpec
import org.scalacheck._
import scala.collection.JavaConversions._
/**
 * Tests for Summingbird's Storm planner.
 */

class TopologyTests extends WordSpec {
  import MapAlgebra.sparseEquiv

  // This is dangerous, obviously. The Storm platform graphs tested
  // here use the UnitBatcher, so the actual time extraction isn't
  // needed.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)
  implicit val batcher = Batcher.unit

  /**
   * The function tested below. We can't generate a function with
   * ScalaCheck, as we need to know the number of tuples that the
   * flatMap will produce.
   */
  val testFn = { i: Int => List((i -> i)) }

  implicit val storm = Storm.local()

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  /**
   * Perform a single run of TestGraphs.singleStepJob using the
   * supplied list of integers and the testFn defined above.
   */
  def funcToPlan(mkJob: (Producer[Storm, Int], Storm#Store[Int, Int]) => TailProducer[Storm, Any]): StormTopology = {
    val original = sample[List[Int]]

    val job = mkJob(
      Storm.source(TraversableSpout(original)),
      TestStore.createStore[Int, Int]()._2
    )

    storm.plan(job).topology
  }

  "Number of bolts should be as expected" in {
    val stormTopo =
      funcToPlan(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_, _)(testFn)
      )
    // Final Flatmap + summer
    assert(stormTopo.get_bolts_size() == 2)
  }

  "Number of spouts in simple task should be 1" in {
    val stormTopo =
      funcToPlan(
        TestGraphs.singleStepJob[Storm, Int, Int, Int](_, _)(testFn)
      )
    // Source producer
    assert(stormTopo.get_spouts_size() == 1)
  }

  /*
  *  Test Case to check the opt-in mechanism on the FlatMapperProducer.
  *  Asserts : the parallelism of the source node
  *            asserts it is a one spout and one bolt topology
  *            assert the summer parallelism too.
  *     source -> flatMap -> sink
  */
  "Opt-in for the flatmap to merge in source." in {
    val fmNodeName = "flatMapper"
    val smNodeName = "summer"
    val sourceNodeName = "source"
    val p = Storm.source(TraversableSpout(sample[List[Int]])).name(sourceNodeName)
      .flatMap(testFn).name(fmNodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2).name(smNodeName)

    val opts = Map(fmNodeName -> Options().set(FMMergeableWithSource(true)).set(FlatMapParallelism(5)),
      sourceNodeName -> Options().set(SourceParallelism(10)),
      smNodeName -> Options().set(SummerParallelism(7)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    val bolts = stormTopo.get_bolts
    val spouts = stormTopo.get_spouts
    assert(bolts.size == 1 && spouts.size == 1)
    assert(bolts("Tail").get_common.get_parallelism_hint == 7)

    val spout = spouts.head._2
    assert(spout.get_common.get_parallelism_hint == 10)
  }

  /*
    Test Case to check the opt-in mechanism on the OptionMappedProducer
    Asserts : There is only one spout and one bolt
              Asserts the summer parallelism
              Asserts the source parallelism
          source -> map -> sink
 */
  "Remove FlatMapNode where TailProducer does not have FlatMapProducer" in {
    val optNodeName = "optionMapper"
    val smNodeName = "summer"
    val sourceNodeName = "source"
    val p = Storm.source(TraversableSpout(sample[List[Int]])).name(sourceNodeName)
      .map(x => (x, 1)).name(optNodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2).name(smNodeName)

    val opts = Map(optNodeName -> Options().set(FMMergeableWithSource(true)).set(FlatMapParallelism(5)),
      sourceNodeName -> Options().set(SourceParallelism(10)),
      smNodeName -> Options().set(SummerParallelism(7)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    val bolts = stormTopo.get_bolts
    val spouts = stormTopo.get_spouts

    assert(stormTopo.get_bolts_size == 1 && stormTopo.get_spouts_size == 1)
    assert(spouts.head._2.get_common.get_parallelism_hint == 10)
    assert(bolts("Tail").get_common.get_parallelism_hint == 7)
  }

  /*
 *  Test Case to check if it errors out with a RuntimeException when the FMMergeableWithSource is opted-in and sourceParallelism is smaller than FMParallelism
 *  Asserts : RuntimeException has occurred when sourceParallelism is smaller than FM Parallelism
 */
  "While FMMergeableWithSource Option is set to true, error out when SourceParallelism is less that FMParallelism" in {
    val fmNodeName = "flatMapper"
    val smNodeName = "summer"
    val sourceNodeName = "source"
    val p = Storm.source(TraversableSpout(sample[List[Int]])).name(sourceNodeName)
      .flatMap(testFn).name(fmNodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2).name(smNodeName)

    val opts = Map(fmNodeName -> Options().set(FMMergeableWithSource(true)).set(FlatMapParallelism(15)),
      sourceNodeName -> Options().set(SourceParallelism(10)),
      smNodeName -> Options().set(SummerParallelism(7)))
    val storm = Storm.local(opts)
    try {
      val stormTopo = storm.plan(p).topology
      assert(false)
    } catch {
      case _: RuntimeException => assert(true)
    }
  }

  /*
  * Test : FlatMap has a fanout and the producer has a opt-in FMMergeableWithSource.
  * Behavior : When a flatMap has a fanOut then the FlatMappedProducer should not go into Source.
  * Asserts : Number of bolts : flatMap, Map, Summer1, Summer2
  *           Number of spouts : source.
  *
  *     source -> flatMap -> sink
  *                       -> map -> sink
  */
  "FMMergeableWithSource with a fanOut case after flatMap" in {
    val sumName = "summer"
    val p1 = Storm.source(TraversableSpout(sample[List[Int]])).flatMap(testFn)
    val p2 = p1.sumByKey(TestStore.createStore[Int, Int]()._2).name("sum1")
    val p3 = p1.map { x => x }.sumByKey(TestStore.createStore[Int, Int]()._2).name("sum2")
    val p = p2.also(p3)
    val opts = Map("sum1" -> Options().set(FMMergeableWithSource(true)).set(FlatMapParallelism(15)),
      "sum2" -> Options().set(SourceParallelism(50)).set(FMMergeableWithSource(true)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    val bolts = stormTopo.get_bolts
    val spouts = stormTopo.get_spouts
    // FlatMapNode is expected to get created when it has a fanOut. Nodes : FlatMap, Map, Summer, Summer
    assert(bolts.size == 4)
    // There should be one spout.
    assert(spouts.size == 1)
  }

  /*
 * Test : FlatMap has a fanout and the producer has a opt-in FMMergeableWithSource.
 * Behavior : When a flatMap has a fanOut then the FlatMappedProducer should not go into Source.
 * Asserts : Number of bolts : flatMap, Map, Summer1, Summer2
 *           Number of spouts : source.
 *
 *     source -> flatMap -> sink
 *                          ^^^
 *     source -> flatMap ----|
 */
  "FMMergeableWithSource with a merge into same sink" in {
    val sumName = "summer"
    val p1 = Storm.source(TraversableSpout(sample[List[Int]])).flatMap(testFn).name("src1")
    val p2 = Storm.source(TraversableSpout(sample[List[Int]])).flatMap(testFn).name("src2")
    val p3 = p1.merge(p2)
    val p4 = p3.sumByKey(TestStore.createStore[Int, Int]()._2).name("summer")
    val opts = Map("summer" -> Options().set(FMMergeableWithSource(true)).set(FlatMapParallelism(15)).set(SummerParallelism(15)),
      "src1" -> Options().set(SourceParallelism(15)).set(FMMergeableWithSource(true)),
      "src2" -> Options().set(SourceParallelism(15)).set(FMMergeableWithSource(true)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p4).topology
    val bolts = stormTopo.get_bolts
    val spouts = stormTopo.get_spouts
    // FlatMapNode is expected to get created when it has a fanOut. Nodes : FlatMap, Map, Summer, Summer
    assert(bolts.size == 1)
    // There should be two spouts where flatMaps are merged into them.
    assert(spouts.size == 2)
  }

  "A named node after a flat map should imply its options" in {
    val nodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(nodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(nodeName -> Options().set(FlatMapParallelism(50)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(1).get_common.get_parallelism_hint == 50)
  }

  "With 2 names in a row we take the closest name" in {
    val nodeName = "super dooper node"
    val otherNodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(nodeName).name(otherNodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(otherNodeName -> Options().set(FlatMapParallelism(40)),
      nodeName -> Options().set(FlatMapParallelism(50)))

    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(1).get_common.get_parallelism_hint == 50)
  }

  "If the closes doesnt contain the option we keep going" in {
    val nodeName = "super dooper node"
    val otherNodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(otherNodeName).name(nodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(otherNodeName -> Options().set(SourceParallelism(30)),
      nodeName -> Options().set(FlatMapParallelism(50)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(1).get_common.get_parallelism_hint == 50)
  }

  "Options propagate backwards" in {
    val nodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(nodeName).name("Throw away name")
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(nodeName -> Options().set(FlatMapParallelism(50)).set(SourceParallelism(30)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts
    val spouts = stormTopo.get_spouts
    val spout = spouts.head._2

    assert(spout.get_common.get_parallelism_hint == 30)
  }

  "Options don't propagate forwards" in {
    val nodeName = "super dooper node"
    val otherNodeName = "super dooper node"
    val p = Storm.source(TraversableSpout(sample[List[Int]]))
      .flatMap(testFn).name(otherNodeName).name(nodeName)
      .sumByKey(TestStore.createStore[Int, Int]()._2)

    val opts = Map(otherNodeName -> Options().set(SourceParallelism(30)).set(SummerParallelism(50)),
      nodeName -> Options().set(FlatMapParallelism(50)))
    val storm = Storm.local(opts)
    val stormTopo = storm.plan(p).topology
    // Source producer
    val bolts = stormTopo.get_bolts

    // Tail will have 1 -, distance from there should be onwards
    val TDistMap = bolts.map { case (k, v) => (k.split("-").size - 1, v) }

    assert(TDistMap(0).get_common.get_parallelism_hint == 5)
  }
}
