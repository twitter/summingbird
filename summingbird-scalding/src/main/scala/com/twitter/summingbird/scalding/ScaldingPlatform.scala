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

import com.twitter.algebird.{Monoid, Semigroup, Monad}
import com.twitter.algebird.Monad.operators // map/flatMap for monads
import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.scalding.{ Tool => STool, _ }
import com.twitter.summingbird._
import com.twitter.summingbird.monad.StateWithError
import com.twitter.summingbird.batch._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.GenericOptionsParser
import java.util.{ Date, HashMap => JHashMap, Map => JMap, TimeZone }
import cascading.flow.FlowDef
import com.twitter.scalding.Mode

object Scalding {
  def source[T](factory: PipeFactory[T])
    (implicit inj: Injection[T, Array[Byte]], manifest: Manifest[T], timeOf: TimeExtractor[T]) =
    Producer.source[Scalding, T](factory)
    // TODO: remove TimeExtractor from Source, move to summingbirdbatch, it's not needed here

  def limitTimes[T](range: Interval[Time], in: FlowToPipe[T]): FlowToPipe[T] =
    in.map { pipe => pipe.filter { case (time, _) => range(time) } }

  def merge[T](left: FlowToPipe[T], right: FlowToPipe[T]): FlowToPipe[T] =
    for { l <- left; r <- right } yield (l ++ r)
}

trait Executor {
  def run(config: Map[String, AnyRef], inargs: Array[String], cons: (Args) => Job): Unit
}

class StandardRunner extends Executor {
  def run(config: Map[String, AnyRef], inargs: Array[String], con: (Args) => Job) {
    val hadoopTool: STool = {
      val tool = new STool
      tool.setJobConstructor(con)
      tool
    }
    ToolRunner.run(ConfigBijection.fromMap(config), hadoopTool, inargs)
  }
}

// TODO
class TestRunner extends Executor {
  def run(config: Map[String, AnyRef], inargs: Array[String], con: (Args) => Job) { }
}

class Scalding(jobName: String, timeSpan: Interval[Time], inargs: Array[String], runner: Executor) extends Platform[Scalding] {
  type Source[T] = PipeFactory[T]
  type Store[K, V] = ScaldingStore[K, V]
  type Service[K, V] = ScaldingService[K, V]
  type Plan[T] = PipeFactory[T]
  /**
    * run(Summer(producer, store))
    *
    * - command line runner gives us the # of batches we want to run.
    * - the store needs to give us the current maximum batch.
    */

  private def buildSummer[K, V](summer: Summer[Scalding, K, V], id: Option[String]): PipeFactory[(K, V)] = {
    val Summer(producer, store, monoid) = summer
    /*
     * The store may already have materialized values, so we don't need the whole
     * input history, but to produce NEW batches, we may need some input.
     * So, we pass the full PipeFactory to to the store so it can request only
     * the time ranges that it needs.
     */
    // TODO: plumb the options through, don't just put -1 and NonCommutative
    store.merge(buildFlow(producer, id), monoid, NonCommutative, -1)
  }

  private def buildJoin[K, V, JoinedV](joined: LeftJoinedProducer[Scalding, K, V, JoinedV],
    id: Option[String]): PipeFactory[(K, (V, Option[JoinedV]))] = {
    val LeftJoinedProducer(left, service) = joined
    /**
     * There is no point loading more from the left than the service can
     * join with, so we pass in the left PipeFactory so that the service
     * can compute how wuch it can actually handle and only load that much
     */
    service.lookup(buildFlow(left, id))
  }

  /** Return a PipeFactory that can cover as much as possible of the time range requested,
   * but the output state gives the actual, non-empty, interval that can be produced
   */
  def buildFlow[T](producer: Producer[Scalding, T], id: Option[String]): PipeFactory[T] = {
    producer match {
      case Source(src, manifest, timeExtractor) => src // Time is added when we create the Source object
      case IdentityKeyedProducer(producer) => buildFlow(producer, id)
      case NamedProducer(producer, newId)  => buildFlow(producer, id = Some(newId))
      case summer@Summer(producer, store, monoid) => buildSummer(summer, id)
      case joiner@LeftJoinedProducer(producer, svc) => buildJoin(joiner, id)
      case OptionMappedProducer(producer, op, manifest) =>
        // Map in two monads here, first state then reader
        buildFlow(producer, id).map { flowP =>
          flowP.map { typedPipe =>
            // TODO make Function1 instances outside to avoid the closure + serialization issues
            typedPipe.flatMap { case (time, item) => op(item).map { (time, _) }.toIterable }
          }
        }
      case FlatMappedProducer(producer, op) =>
        // Map in two monads here, first state then reader
        buildFlow(producer, id).map { flowP =>
          flowP.map { typedPipe =>
            // TODO remove toIterable in scalding 0.9.0
            // TODO make Function1 instances outside to avoid the closure + serialization issues
            typedPipe.flatMap { case (time, item) => op(item).toIterable.view.map { (time, _) } }
          }
        }
      case MergedProducer(l, r) => {
        for {
          // concatenate errors (++) and find the intersection (&&) of times
          leftAndRight <- buildFlow(l, id).join(buildFlow(r, id),
            { (lerr: List[FailureReason], rerr: List[FailureReason]) => lerr ++ rerr },
            { case ((tsl, leftFM), (tsr, _)) => (tsl && tsr, leftFM) })
          merged = Scalding.merge(leftAndRight._1, leftAndRight._2)
          maxAvailable <- StateWithError.getState // read the latest state, which is the time
        } yield Scalding.limitTimes(maxAvailable._1, merged)
      }
    }
  }

  /**
    * Base hadoop config instances used by the Scalding platform.
    */
  def baseConfig = new Configuration

  def config: Map[String, AnyRef] = sys.error("TODO, set up the kryo serializers")

  def plan[T](prod: Producer[Scalding, T]): PipeFactory[T] =
    buildFlow(prod, None)

  def run(pf: PipeFactory[_]): Unit = {
    val constructor = { (jobArgs : Args) =>
      new PipeFactoryJob(jobName, timeSpan, pf, { () => () }, jobArgs)
    }
    // Now actually run (by building the scalding job):
    try {
      runner.run(config, inargs, constructor)
    } catch {
      case ise: InvalidSourceException => println("Job data not ready: " + ise.getMessage)
    }
  }
}

/** Build a scalding job that tries to run for the given interval of time. It may run for less than this interval,
 * but if it cannot move forward at all, it will fail.
 */
class PipeFactoryJob[T](override val name: String, times: Interval[Time], pf: PipeFactory[T], onSuccess: Function0[Unit], args: Args) extends Job(args) {

  // Build the flowDef:
  pf((times, implicitly[Mode])) match {
    case Left(failures) =>
       // TODO this should be better
       // We could compute the earliest time we could possibly run, and then return
       // that to reschedule, for one thing. But we are smart, we can make it much better.
       failures.foreach { println(_) }
       sys.error("Couldn't schedule: \n" + failures.mkString("\n"))
    // TODO, maybe log the upper bound of the time
    case Right(((timeSpanWeRun, mode), flowToPipe)) => flowToPipe((implicitly[FlowDef], mode))
  }

  /* TODO can't this be handled outside by setting up the conf we pass to ToolRunner?
    * Replace Scalding's default implementation of
    *  cascading.kryo.KryoSerialization with Summingbird's custom
    * extension. SummingbirdKryoHadoop performs every registration in
    * KryoHadoop, then registers event, time, key and value codecs
    * using chill's BijectiveSerializer.
    */
  override def config(implicit mode: Mode) =
    super.config(mode) ++ Map(
      "io.serializations" ->
        (ioSerializations ++ List(classOf[SummingbirdKryoHadoop].getName)).mkString(",")
    )

  override def next = {
    onSuccess.apply()
    None
  }
}
