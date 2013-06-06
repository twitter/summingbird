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

case class ScaldingSerialization[T](injectionPair: InjectionPair[T]) extends Serialization[Scalding, T]

// TODO this functionality should be in algebird
sealed trait Commutativity extends java.io.Serializable
object NonCommutative extends Commutativity
object Commutative extends Commutativity

trait ScaldingStore[K, V] extends Store[Scalding, K, V] {

  def ordering: Ordering[K]

  def writeDeltas(batchID: BatchID, delta: KeyValuePipe[K, V])(implicit flowdef: FlowDef, mode: Mode): Unit

  /** Optionally write out the stream of output values (for use as a service)
   */
  def writeStream(batchID: BatchID, scanned: KeyValuePipe[K, V])(implicit flowdef: FlowDef, mode: Mode): Unit
  /** Write the stream which only has the last value for each key
   */
  def writeLast(batchID: BatchID, scanned: KeyValuePipe[K, V])(implicit flowdef: FlowDef, mode: Mode): Unit

  /** Read the latest value for the keys
   * Each key appears at most once
   * May include keys from previous batches if those keys have not been updated
   */
  def readLatestBefore(batchID: BatchID)(implicit flowdef: FlowDef, mode: Mode): KeyValuePipe[K, V]

  /**
    * Accepts deltas along with their timestamps, returns triples of
    * (time, K, V(aggregated up to the time)).
    *
    * Same return as lookup on a ScaldingService.
    */
  def merge(batchID: BatchID,
    delta: KeyValuePipe[K, V],
    commutativity: Commutativity,
    reducers: Int = -1)(implicit flowdef: FlowDef, mode: Mode, sg: Semigroup[V]): KeyValuePipe[K, V] = {

    writeDeltas(batchID, delta)
   /**
    * The job merges in new data as an incremental update to some
    * existing dataset. Because the existing data is pre-aggregated,
    * we don't have times for any of the key-value pairs. If the
    * Monoid[V] is not commutative, this is cause for concern; how do
    * we ensure that old data always appears before new data in the
    * sort?
    *
    * We solve this by sorting on Option[Long] instead of Long,
    * tagging all old data with None and lifting every timestamp in
    * new key-value pairs up to Some(timestamp). Because None <
    * Some(x), the data will be sorted correctly.
    */
    implicit val ord: Ordering[K] = ordering

    def toGrouped(items: KeyValuePipe[K, V]): Grouped[K, (Long, V)] =
      items.groupBy { case (_, (k, _)) => k }
        .mapValues { case (t, (_, v)) => (t, v) }
        .withReducers(reducers)

    def toKVPipe(tp: TypedPipe[(K, (Long, V))]): KeyValuePipe[K, V] =
      tp.map { case (k, (t, v)) => (t, (k, v)) }

    val grouped: Grouped[K, (Long, V)] = toGrouped(readLatestBefore(batchID) ++ delta)

    val sorted = grouped.sortBy { _._1 } // sort by time
    val maybeSorted = commutativity match {
      case Commutative => grouped // order does not matter
      case NonCommutative => sorted
    }

    val redFn: (((Long, V), (Long, V)) => (Long, V)) = { (left, right) =>
      val (tl, vl) = left
      val (tr, vr) = right
      (tl max tr, Semigroup.plus(vl, vr))
    }

    writeLast(batchID, toKVPipe(maybeSorted.reduce(redFn)))

    // Make the incremental stream
    val stream = toKVPipe(sorted.scanLeft(None: Option[(Long, V)]) { (old, item) =>
        old match {
          case None => Some(item)
          case Some(prev) => Some(redFn(prev, item))
        }
      }
      .mapValueStream { _.flatten /* unbox the option */ }
      .toTypedPipe
    )
    writeStream(batchID, stream)
    stream
  }
}

trait ScaldingService[K, V] extends Service[Scalding, K, V] {
  def ordering: Ordering[K]
  /** Reads the key log for this batch
   * May include keys from previous batches if those keys have not been updated
   * since
   */
  def readStream(batchID: BatchID)(implicit flowdef: FlowDef, mode: Mode): KeyValuePipe[K, V]

  /** TODO Move the time join logic here
   */
  def lookup[W](batchID: BatchID, getKeys: KeyValuePipe[K, W])(implicit flowdef: FlowDef, mode: Mode): KeyValuePipe[K, (W, Option[V])]
}

object Scalding {
  implicit def ser[T](implicit inj: Injection[T, Array[Byte]], mf: Manifest[T]): Serialization[Scalding, T] = {
    ScaldingSerialization(InjectionPair(mf.erasure.asInstanceOf[Class[T]], inj))
  }

  def source[T](factory: PipeFactory[T])
    (implicit inj: Injection[T, Array[Byte]], manifest: Manifest[T], timeOf: TimeExtractor[T]) =
    Producer.source[Scalding, T](factory)
}

class Scalding(jobName: String, batchID: BatchID, inargs: Array[String]) extends Platform[Scalding] {
  type Source[T] = PipeFactory[T]

  /**
    * run(Summer(producer, store))
    *
    * - command line runner gives us the # of batches we want to run.
    * - the store needs to give us the current maximum batch.
    */

  private def buildSummer[K, V](summer: Summer[Scalding, K, V], id: Option[String]): PipeFactory[(K, V)] = {
    val Summer(producer, store, kSer, vSer, monoid, batcher) = summer
    val sstore = store.asInstanceOf[ScaldingStore[K, V]]
    // The scala compiler gets confused if we don't capture this in a val:
    // [error] /Users/oscarb/workspace/summingbird/summingbird-scalding/src/main/scala/com/twitter/summingbird/scalding/ScaldingPlatform.scala:158: com.twitter.summingbird.scalding.ScaldingStore[K,V] does not take parameters
    val pf = { (b: BatchID, fd: FlowDef, mode: Mode) =>
      val inPipe: KeyValuePipe[K, V] = buildFlow(producer, id).apply(b, fd, mode)
      // TODO get the options for this id to get Commutative and reducers
      sstore.merge(b, inPipe, Commutative)(fd, mode, monoid)
    }
    pf
  }

  private def buildJoin[K, V, JoinedV](joined: LeftJoinedProducer[Scalding, K, V, JoinedV],
    id: Option[String]): PipeFactory[(K, (V, Option[JoinedV]))] = {
    val LeftJoinedProducer(left, service) = joined
    val sservice = service.asInstanceOf[ScaldingService[K, JoinedV]]
    val pf = { (b: BatchID, fd: FlowDef, mode: Mode) =>
      val inPipe: KeyValuePipe[K, V] = buildFlow(left, id).apply(b, fd, mode)
      sservice.lookup(b, inPipe)(fd, mode)
    }
    pf
  }

  def buildFlow[T](producer: Producer[Scalding, T], id: Option[String]): PipeFactory[T] = {

    producer match {
      case Source(src, _, _) => src
      case IdentityKeyedProducer(producer) => buildFlow(producer, id)
      case NamedProducer(producer, newId)  => buildFlow(producer, id = Some(newId))
      case summer@Summer(producer, store, kSer, vSer, monoid, batcher) => buildSummer(summer, id)
      case joiner@LeftJoinedProducer(producer, svc) => buildJoin(joiner, id)
      case OptionMappedProducer(producer, op) => buildFlow(producer, id).map { tp =>
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

    val hadoopTool: STool = {
      val tool = new STool
      tool.setJobConstructor { jobArgs => new PipeFactoryJob(jobName, batchID, pf, { () => () }, jobArgs) }
      tool
    }

    // Now actually run (by building the scalding job):
    try {
      ToolRunner.run(ConfigBijection.fromMap(config), hadoopTool, inargs);
    } catch {
      case ise: InvalidSourceException => println("Job data not ready: " + ise.getMessage)
    }
  }
}

class PipeFactoryJob[T](override val name: String, batchID: BatchID, pf: PipeFactory[T], onSuccess: Function0[Unit], args: Args) extends Job(args) {

  // Build the flowDef:
  pf.apply(batchID, implicitly[FlowDef], implicitly[Mode])
  /*
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
