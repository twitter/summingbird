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

package com.twitter.summingbird.builder

import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.scalding.TypedPipe
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.scalding.{Scalding, ScaldingService, ScaldingEnv, BatchedScaldingStore, ScaldingSink}
import com.twitter.summingbird.service.CompoundService
import com.twitter.summingbird.sink.{CompoundSink, BatchedSinkFromOffline}
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.store.CompoundStore
import com.twitter.summingbird.storm.{ MergeableStoreSupplier, StoreWrapper, Storm, StormEnv }
import com.twitter.summingbird.util.CacheSize
import java.io.Serializable
import java.util.{ Date, UUID }

/**
  * The (deprecated) Summingbird builder API builds up a single
  * MapReduce job using a SourceBuilder. After any number of calls to
  * flatMap, leftJoin, filter, merge, etc, the user calls
  * "groupAndSumTo", equivalent to "sumByKey" in the Producer
  * API. This call converts the SourceBuilder into a CompletedBuilder
  * and prevents and future flatMap operations.
  *
  * @author Oscar Boykin
  * @author Sam Ritchie
  * @author Ashu Singhal
  */

object SourceBuilder {
  type PlatformPair = OptionalPlatform2[Scalding, Storm]
  type Node[T] = Producer[PlatformPair, T]

  def freshUUID: String = UUID.randomUUID.toString
  def adjust[T](m: Map[T, Options], k: T)(f: Options => Options) =
    m.updated(k, f(m.getOrElse(k, Options())))

  implicit def sg[T]: Semigroup[SourceBuilder[T]] =
    Semigroup.from(_ ++ _)

  def apply[T](eventSource: EventSource[T], timeOf: T => Date)
    (implicit mf: Manifest[T], eventCodec: Injection[T, Array[Byte]]) = {
    implicit val te = TimeExtractor[T](timeOf(_).getTime)
    val newID = freshUUID
    val scaldingSource =
      eventSource.offline.map( s => Scalding.pipeFactory(s.scaldingSource(_)))
    val stormSource = eventSource.spout.map(Storm.timedSpout(_))
    new SourceBuilder[T](
      Source[PlatformPair, T]((scaldingSource, stormSource), manifest),
      List(CompletedBuilder.injectionPair[T](eventCodec)),
      newID
    )
  }
}

case class SourceBuilder[T: Manifest] private (
  node: SourceBuilder.Node[T],
  pairs: List[InjectionPair[_]],
  id: String,
  opts: Map[String, Options] = Map.empty
) extends Serializable {
  import SourceBuilder.{ adjust, Node }

  def map[U: Manifest](fn: T => U): SourceBuilder[U] = copy(node = node.map(fn))
  def filter(fn: T => Boolean): SourceBuilder[T] = copy(node = node.filter(fn))
  def flatMap[U: Manifest](fn: T => TraversableOnce[U]): SourceBuilder[U] =
    copy(node = node.flatMap(fn))
  def flatMapBuilder[U: Manifest](newFlatMapper: FlatMapper[T, U]): SourceBuilder[U] =
    flatMap(newFlatMapper(_))

  def write[U](sink: CompoundSink[U])(conversion: T => TraversableOnce[U])
    (implicit batcher: Batcher, mf: Manifest[U]): SourceBuilder[T] = {
    val newNode =
      node.flatMap(conversion).write(
        sink.offline.map(new BatchedSinkFromOffline[U](batcher, _)),
        sink.online
      )
    copy(
      node = node.either(newNode).flatMap[T] {
        case Left(t) => Some(t)
        case Right(u) => None
      }
    )
  }

  def leftJoin[K, V, JoinedValue](service: CompoundService[K, JoinedValue])
    (implicit ev: T <:< (K, V), keyMf: Manifest[K], valMf: Manifest[V], joinedMf: Manifest[JoinedValue])
      : SourceBuilder[(K, (V, Option[JoinedValue]))] =
    copy(
      node = node.asInstanceOf[Node[(K, V)]].leftJoin(
        service.offline,
        service.online.map(StoreWrapper[K, JoinedValue](_))
      )
    )

  def set(opt: Any): SourceBuilder[T] = copy(opts = adjust(opts, id)(_.set(opt)))

  /**
    * Complete this builder instance with a BatchStore. At this point,
    * the Summingbird job can be executed on Hadoop.
    */
  def groupAndSumTo[K, V](store: BatchedScaldingStore[K, V])(
    implicit ev: T <:< (K, V),
    env: Env,
    keyMf: Manifest[K],
    valMf: Manifest[V],
    keyCodec: Injection[K, Array[Byte]],
    valCodec: Injection[V, Array[Byte]],
    batcher: Batcher,
    monoid: Monoid[V]): CompletedBuilder[_, K, V] =
    groupAndSumTo(CompoundStore.fromOffline(store))

  /**
    * Complete this builder instance with a MergeableStore. At this point,
    * the Summingbird job can be executed on Storm.
    */
  def groupAndSumTo[K, V](store: => MergeableStore[(K, BatchID), V])(
    implicit ev: T <:< (K, V),
    env: Env,
    keyMf: Manifest[K],
    valMf: Manifest[V],
    keyCodec: Injection[K, Array[Byte]],
    valCodec: Injection[V, Array[Byte]],
    batcher: Batcher,
    monoid: Monoid[V]): CompletedBuilder[_, K, V] =
    groupAndSumTo(CompoundStore.fromOnline(store))

  /**
    * Complete this builder instance with a CompoundStore. At this
    * point, the Summingbird job can be executed on Storm or Hadoop.
    */
  def groupAndSumTo[K, V](store: CompoundStore[K, V])(
    implicit ev: T <:< (K, V),
    env: Env,
    keyMf: Manifest[K],
    valMf: Manifest[V],
    keyCodec: Injection[K, Array[Byte]],
    valCodec: Injection[V, Array[Byte]],
    batcher: Batcher,
    monoid: Monoid[V]): CompletedBuilder[_, K, V] = {

    val cb = env match {
      case scalding: ScaldingEnv =>
        val givenStore = store.offlineStore.getOrElse(sys.error("No offline store given in Scalding mode"))
        // Set the store to reset if needed
        val batchSetStore = scalding
          .initialBatch(batcher)
          .map { givenStore.withInitialBatch(_) }
          .getOrElse(givenStore)

        val newNode = OptionalUnzip2[Scalding, Storm]()(node)._1.map {
          Producer.evToKeyed(_)
            .name(id)
            .sumByKey(batchSetStore)
        }.getOrElse(sys.error("Scalding mode specified alongside some online-only Source, Service or Sink."))
        CompletedBuilder(newNode, pairs, batcher, keyCodec, valCodec, SourceBuilder.freshUUID, opts)

      case storm: StormEnv =>
        val givenStore = MergeableStoreSupplier.from {
          store.onlineSupplier
            .getOrElse(sys.error("No online store given in Storm mode"))
            .apply()
          }

        val newNode = OptionalUnzip2[Scalding, Storm]()(node)._2.map {
          Producer.evToKeyed(_)
            .name(id)
            .sumByKey(givenStore)
        }.getOrElse(sys.error("Storm mode specified alongside some offline-only Source, Service or Sink."))
        CompletedBuilder(newNode, pairs, batcher, keyCodec, valCodec, Storm.SINK_ID, opts)

      case _ => sys.error("Unknown environment: " + env)
    }
    env.builder = cb
    cb
  }

  // useful when you need to merge two different Event sources
  def ++(other: SourceBuilder[T]): SourceBuilder[T] =
    copy(
      node = node.name(id).merge(other.node.name(other.id)),
      pairs = pairs ++ other.pairs,
      id = SourceBuilder.freshUUID,
      opts = opts ++ other.opts
    )
}
