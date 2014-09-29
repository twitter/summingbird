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

import com.twitter.algebird.{ Monoid, Semigroup }
import com.twitter.bijection.{ Codec, Injection }
import com.twitter.chill.IKryoRegistrar
import com.twitter.chill.java.IterableRegistrar
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.scalding.{ Scalding, Service, ScaldingEnv, Sink }
import com.twitter.summingbird.scalding.batch.BatchedStore
import com.twitter.summingbird.service.CompoundService
import com.twitter.summingbird.sink.{ CompoundSink, BatchedSinkFromOffline }
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.store.CompoundStore
import com.twitter.summingbird.online._
import com.twitter.summingbird.storm.{
  Storm,
  StormEnv,
  StormSource,
  StormSink
}
import java.io.Serializable
import java.util.Date

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

  private val nextId = new java.util.concurrent.atomic.AtomicLong(0)

  def adjust[T](m: Map[T, Options], k: T)(f: Options => Options) =
    m.updated(k, f(m.getOrElse(k, Options())))

  implicit def sg[T]: Semigroup[SourceBuilder[T]] =
    Semigroup.from(_ ++ _)

  def nextName[T: Manifest]: String =
    "%s_%d".format(manifest[T], nextId.getAndIncrement)

  def apply[T](eventSource: EventSource[T], timeOf: T => Date)(implicit mf: Manifest[T], eventCodec: Codec[T]) = {
    implicit val te = TimeExtractor[T](timeOf(_).getTime)
    val newID = nextName[T]
    val scaldingSource =
      eventSource.offline.map(s => Scalding.pipeFactory(s.scaldingSource(_)))
    val stormSource = eventSource.spout.map(Storm.toStormSource(_))
    new SourceBuilder[T](
      Source[PlatformPair, T]((scaldingSource, stormSource)),
      CompletedBuilder.injectionRegistrar[T](eventCodec),
      newID
    )
  }
}

case class SourceBuilder[T: Manifest] private (
    @transient node: SourceBuilder.Node[T],
    @transient registrar: IKryoRegistrar,
    id: String,
    @transient opts: Map[String, Options] = Map.empty) extends Serializable {
  import SourceBuilder.{ adjust, Node, nextName }

  def map[U: Manifest](fn: T => U): SourceBuilder[U] = copy(node = node.map(fn))
  def filter(fn: T => Boolean): SourceBuilder[T] = copy(node = node.filter(fn))
  def flatMap[U: Manifest](fn: T => TraversableOnce[U]): SourceBuilder[U] =
    copy(node = node.flatMap(fn))

  /**
   * This may be more efficient if you know you are not changing the values in
   * you flatMap.
   */
  def flatMapKeys[K1, K2, V](fn: K1 => TraversableOnce[K2])(implicit ev: T <:< (K1, V),
    key1Mf: Manifest[K1], key2Mf: Manifest[K2], valMf: Manifest[V]): SourceBuilder[(K2, V)] =
    copy(node = node.asInstanceOf[Node[(K1, V)]].flatMapKeys(fn))

  def flatMapBuilder[U: Manifest](newFlatMapper: FlatMapper[T, U]): SourceBuilder[U] =
    flatMap(newFlatMapper(_))

  def write[U](sink: CompoundSink[U])(conversion: T => TraversableOnce[U])(implicit batcher: Batcher, mf: Manifest[U]): SourceBuilder[T] = {
    val newNode =
      node.flatMap(conversion).write(
        sink.offline.map(new BatchedSinkFromOffline[U](batcher, _)),
        sink.online.map { supplier => new StormSink[U] { lazy val toFn = supplier() } }
      )
    copy(
      node = node.either(newNode).flatMap[T] {
        case Left(t) => Some(t)
        case Right(u) => None
      }
    )
  }

  def write(sink: CompoundSink[T])(implicit batcher: Batcher): SourceBuilder[T] =
    copy(
      node = node.write(
        sink.offline.map(new BatchedSinkFromOffline[T](batcher, _)),
        sink.online.map { supplier => new StormSink[T] { lazy val toFn = supplier() } }
      )
    )

  def leftJoin[K, V, JoinedValue](service: CompoundService[K, JoinedValue])(implicit ev: T <:< (K, V), keyMf: Manifest[K], valMf: Manifest[V], joinedMf: Manifest[JoinedValue]): SourceBuilder[(K, (V, Option[JoinedValue]))] =
    copy(
      node = node.asInstanceOf[Node[(K, V)]].leftJoin((
        service.offline,
        service.online.map { fn: Function0[ReadableStore[K, JoinedValue]] =>
          ReadableServiceFactory(fn)
        }
      ))
    )

  /** Set's an Option on all nodes ABOVE this point */
  def set(opt: Any): SourceBuilder[T] = copy(opts = adjust(opts, id)(_.set(opt)))

  /**
   * Complete this builder instance with a BatchStore. At this point,
   * the Summingbird job can be executed on Hadoop.
   */
  def groupAndSumTo[K, V](store: BatchedStore[K, V])(
    implicit ev: T <:< (K, V),
    env: Env,
    keyMf: Manifest[K],
    valMf: Manifest[V],
    keyCodec: Codec[K],
    valCodec: Codec[V],
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
    keyCodec: Codec[K],
    valCodec: Codec[V],
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
    keyCodec: Codec[K],
    valCodec: Codec[V],
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

        val newNode = OptionalUnzip2[Scalding, Storm]()(node)._1.map { p =>
          Producer.evToKeyed(p.name(id))
            .sumByKey(batchSetStore)
        }.getOrElse(sys.error("Scalding mode specified alongside some online-only Source, Service or Sink."))
        CompletedBuilder(newNode, registrar, batcher, keyCodec, valCodec, nextName[(K, V)], opts)

      case storm: StormEnv =>
        val supplier = store.onlineSupplier.getOrElse(sys.error("No online store given in Storm mode"))
        val givenStore = MergeableStoreFactory.from(supplier())

        val newNode = OptionalUnzip2[Scalding, Storm]()(node)._2.map { p =>
          Producer.evToKeyed(p.name(id))
            .sumByKey(givenStore)
        }.getOrElse(sys.error("Storm mode specified alongside some offline-only Source, Service or Sink."))
        CompletedBuilder(newNode, registrar, batcher, keyCodec, valCodec, nextName[(K, V)], opts)

      case _ => sys.error("Unknown environment: " + env)
    }
    env.builder = cb
    cb
  }

  // useful when you need to merge two different Event sources
  def ++(other: SourceBuilder[T]): SourceBuilder[T] =
    copy(
      node = node.name(id).merge(other.node.name(other.id)),
      registrar = new IterableRegistrar(registrar, other.registrar),
      id = "merge_" + nextName[T],
      opts = opts ++ other.opts
    )
}
