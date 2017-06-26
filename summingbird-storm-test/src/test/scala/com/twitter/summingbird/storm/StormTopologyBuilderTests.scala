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

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.ReadableServiceFactory
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.planner.OnlinePlan
import com.twitter.summingbird.storm.builder.{EdgeGrouping, Topology}
import com.twitter.summingbird.storm.spout.TraversableSpout
import org.scalacheck._
import org.scalatest.WordSpec

class StormTopologyBuilderTests extends WordSpec {
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

  def sample[T: Arbitrary]: T = Arbitrary.arbitrary[T].sample.get

  "Grouped leftJoin creates fields grouping" in {
    val leftJoinName = "leftJoin"
    val producer: TailProducer[Storm, _] =
      Storm.source(TraversableSpout(sample[List[Int]]))
        .flatMap(testFn)
        .leftJoin(
          ReadableServiceFactory[Int, Int](() => ReadableStore.fromFn(v => Some(v)))
        ).name(leftJoinName)
        .map { case (key, (value, optPrevValue)) => (key, value) }
        .sumByKey(TestStore.createStore[Int, Int]()._2)

    val topologyWithoutGrouping = buildTopology(producer)
    assert(topologyWithoutGrouping.spouts.size == 1)
    assert(topologyWithoutGrouping.bolts.size == 2)
    assert(topologyWithoutGrouping.edges.map(_.grouping).forall { grouping =>
      grouping == EdgeGrouping.Shuffle ||
        grouping == EdgeGrouping.Fields(List(EdgeFormats.ShardKey))
    })

    val topologyWithGrouping = buildTopology(producer, Map[String, Options](
      leftJoinName -> Options().set(LeftJoinGrouping.Grouped)
    ))
    assert(topologyWithGrouping.spouts.size == 1)
    assert(topologyWithGrouping.bolts.size == 3)
    assert(topologyWithGrouping.edges.map(_.grouping).exists { grouping =>
      grouping == EdgeGrouping.Fields(List(EdgeFormats.Key))
    })
  }

  private def buildTopology(producer: TailProducer[Storm, _], options: Map[String, Options] = Map()): Topology = {
    val storm = Storm.local(options)
    val dag = OnlinePlan(producer, options)
    StormTopologyBuilder(options, JobId("test"), dag).build
  }
}
