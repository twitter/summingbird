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

import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.scalding.{ Tool => STool, _ }
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
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

class Scalding(jobName: String, batchID: BatchID, inargs: Array[String], runner: Executor) extends Platform[Scalding] {
  type Source[T] = PipeFactory[T]
  type Store[K, V] = ScaldingStore[K, V]
  type Service[K, V] = ScaldingService[K, V]

  /**
    * run(Summer(producer, store))
    *
    * - command line runner gives us the # of batches we want to run.
    * - the store needs to give us the current maximum batch.
    */

  private def buildSummer[K, V](summer: Summer[Scalding, K, V], id: Option[String]): PipeFactory[(K, V)] = {
    val Summer(producer, store, monoid, batcher) = summer
    // The scala compiler gets confused if we don't capture this in a val:
    // [error] /Users/oscarb/workspace/summingbird/summingbird-scalding/src/main/scala/com/twitter/summingbird/scalding/ScaldingPlatform.scala:158: com.twitter.summingbird.scalding.ScaldingStore[K,V] does not take parameters
    val pf = { (b: BatchID, fd: FlowDef, mode: Mode) =>
      val inPipe: KeyValuePipe[K, V] = buildFlow(producer, id).apply(b, fd, mode)
      // TODO get the options for this id to get Commutative and reducers
      store.merge(b, inPipe, Commutative)(fd, mode, monoid)
    }
    pf
  }

  private def buildJoin[K, V, JoinedV](joined: LeftJoinedProducer[Scalding, K, V, JoinedV],
    id: Option[String]): PipeFactory[(K, (V, Option[JoinedV]))] = {
    val LeftJoinedProducer(left, service) = joined
    val pf = { (b: BatchID, fd: FlowDef, mode: Mode) =>
      val inPipe: KeyValuePipe[K, V] = buildFlow(left, id).apply(b, fd, mode)
      service.lookup(b, inPipe)(fd, mode)
    }
    pf
  }

  def buildFlow[T](producer: Producer[Scalding, T], id: Option[String]): PipeFactory[T] = {
    producer match {
      case Source(src, _, _) => src
      case IdentityKeyedProducer(producer) => buildFlow(producer, id)
      case NamedProducer(producer, newId)  => buildFlow(producer, id = Some(newId))
      case summer@Summer(producer, store, monoid, batcher) => buildSummer(summer, id)
      case joiner@LeftJoinedProducer(producer, svc) => buildJoin(joiner, id)
      case OptionMappedProducer(producer, op, manifest) => buildFlow(producer, id).map { tp =>
        tp.flatMap { case (time, item) => op(item).map { (time, _) } }
      }
      case FlatMappedProducer(producer, op) => buildFlow(producer, id).map { tp =>
        tp.flatMap { case (time, item) => op(item).map { (time, _) }.toIterable } // TODO remove toIterable in scalding 0.9.0
      }
      case MergedProducer(l, r) => for {
        pipe1 <- buildFlow(l, id)
        pipe2 <- buildFlow(r, id)
      } yield (pipe1 ++ pipe2)
    }
  }

  /**
    * Base hadoop config instances used by the Scalding platform.
    */
  def baseConfig = new Configuration

  def config: Map[String, AnyRef] = sys.error("TODO, set up the kryo serializers")
  def run[K, V](summer: Summer[Scalding, K, V]): Unit = {
    val pf = buildFlow(summer, None)

    val constructor = { (jobArgs : Args) =>
      new PipeFactoryJob(jobName, batchID, pf, { () => () }, jobArgs)
    }
    // Now actually run (by building the scalding job):
    try {
      runner.run(config, inargs, constructor)
    } catch {
      case ise: InvalidSourceException => println("Job data not ready: " + ise.getMessage)
    }
  }
}

class PipeFactoryJob[T](override val name: String, batchID: BatchID, pf: PipeFactory[T], onSuccess: Function0[Unit], args: Args) extends Job(args) {

  // Build the flowDef:
  pf.apply(batchID, implicitly[FlowDef], implicitly[Mode])
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
