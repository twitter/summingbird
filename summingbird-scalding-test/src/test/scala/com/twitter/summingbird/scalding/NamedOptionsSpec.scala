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

package com.twitter.summingbird.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.property.ConfigDef
import cascading.property.ConfigDef.Setter
import cascading.tuple.Fields
import com.twitter.scalding.{ Test => TestMode, _ }
import com.twitter.summingbird._
import com.twitter.summingbird.batch.option.Reducers
import org.scalatest.WordSpec

/**
 * Tests for application of named options.
 */
class NamedOptionsSpec extends WordSpec {

  private val ReducerKey = "mapred.reduce.tasks"
  private val FlatMapNodeName1 = "FM1"
  private val FlatMapNodeName2 = "FM2"
  private val SummerNodeName1 = "SM1"
  private val SummerNodeName2 = "SM2"

  private val IdentitySink = new Sink[Int] {
    override def write(incoming: PipeFactory[Int]): PipeFactory[Int] = incoming
  }

  implicit def timeExtractor[T <: (Int, _)] =
    new TimeExtractor[T] {
      override def apply(t: T) = t._1.toLong
    }

  def pipeConfig(pipe: Pipe): Map[String, String] = {
    val configCollector = new Setter {
      var config = Map.empty[String, String]
      override def set(key: String, value: String): String = { config += key -> value; "" }
      override def get(key: String): String = ???
      override def update(key: String, value: String): String = ???
    }

    def recurse(p: Pipe): Unit = {
      val cfg = p.getStepConfigDef
      if (!cfg.isEmpty) {
        cfg.apply(ConfigDef.Mode.REPLACE, configCollector)
      }
      p.getPrevious.foreach(recurse(_))
    }

    recurse(pipe)
    configCollector.config
  }

  def verify[T](
    options: Map[String, Options],
    expectedReducers: Int)(
      jobGen: (Producer[Scalding, (Int, Int)], scalding.Store[Int, Int]) => TailProducer[Scalding, Any]) = {

    val src = Scalding.sourceFromMappable { dr => IterableSource(List.empty[(Int, Int)]) }
    val store = TestStore[Int, Int]("store", TestUtil.simpleBatcher, Map.empty[Int, Int], Long.MaxValue)
    val job = jobGen(src, store)
    val interval = TestUtil.toTimeInterval(1L, Long.MaxValue)

    val scaldingPlatform = Scalding("named options test", options)
    val mode: Mode = TestMode(t => (store.sourceToBuffer).get(t))

    val flowToPipe = scaldingPlatform
      .plan(job)
      .apply((interval, mode))
      .right
      .get
      ._2

    val fd = new FlowDef
    val typedPipe = flowToPipe.apply((fd, mode))
    def tupleSetter[T] = new TupleSetter[T] {
      override def apply(arg: T) = {
        val tup = cascading.tuple.Tuple.size(1)
        tup.set(0, arg)
        tup
      }
      override def arity = 1
    }
    val pipe = typedPipe.toPipe(new Fields("0"))(fd, mode, tupleSetter)
    println(pipeConfig(pipe))
    val numReducers = pipeConfig(pipe)(ReducerKey).toInt
    assert(numReducers === expectedReducers)
  }

  "The ScaldingPlatform" should {
    "use named option for the correct node even if same option defined for previous node" in {
      val fmReducers = 50
      val smReducers = 100

      val options = Map(
        FlatMapNodeName1 -> Options().set(Reducers(fmReducers)),
        SummerNodeName1 -> Options().set(Reducers(smReducers)))

      verify(options, smReducers) { (source, store) =>
        source
          .flatMap(Some(_)).name(FlatMapNodeName1)
          .sumByKey(store).name(SummerNodeName1)
      }
    }

    "use named option from the closest node when two names defined one after the other" in {
      val smReducers1 = 50
      val smReducers2 = 100

      val options = Map(
        SummerNodeName1 -> Options().set(Reducers(smReducers1)),
        SummerNodeName2 -> Options().set(Reducers(smReducers2)))

      verify(options, smReducers1) { (source, store) =>
        source
          .flatMap(Some(_))
          .sumByKey(store).name(SummerNodeName1).name(SummerNodeName2)
      }
    }

    "use named option from the closest upstream node if option not defined on current node" in {
      val fmReducers1 = 50
      val fmReducers2 = 100

      val options = Map(
        FlatMapNodeName1 -> Options().set(Reducers(fmReducers1)),
        FlatMapNodeName2 -> Options().set(Reducers(fmReducers2)))

      verify(options, fmReducers2) { (source, store) =>
        source
          .flatMap(Some(_)).name(FlatMapNodeName1)
          .sumByKey(store).name(SummerNodeName1)
          .map { case (k, (optV, v)) => k }.name(FlatMapNodeName2)
          .write(IdentitySink)
      }
    }
  }
}
