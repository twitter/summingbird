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
import com.twitter.summingbird.scalding.{Scalding, ScaldingService, ScaldingStore, ScaldingSink}
import com.twitter.summingbird.service.CompoundService
import com.twitter.summingbird.sink.{CompoundSink, BatchedSinkFromOffline}
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.store.CompoundStore
import com.twitter.summingbird.storm.{MergeableStoreSupplier, StoreWrapper, Storm, StormOptions}
import com.twitter.summingbird.util.CacheSize
import java.io.Serializable
import java.util.{ Date, UUID }

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// The SourceBuilder is the first level of the expansion.

object SourceBuilder {
  def freshUUID: String = UUID.randomUUID.toString
  def adjust[A, B](m: Map[A, B], k: A)(f: B => B) = m.updated(k, f(m(k)))

  implicit def sg[T]: Semigroup[SourceBuilder[T]] =
    Semigroup.from(_ ++ _)

  def apply[T](eventSource: EventSource[T], timeOf: T => Date)
    (implicit mf: Manifest[T], eventCodec: Injection[T, Array[Byte]]) = {
    implicit val te = TimeExtractor[T](timeOf(_).getTime)
    val newID = freshUUID
    new SourceBuilder[T](
      PairedProducer(
        Scalding.sourceFromMappable(eventSource.offline.get.scaldingSource(_)).name(newID),
        Storm.source(eventSource.spout.get).name(newID)
      ),
      List(CompletedBuilder.injectionPair[T](eventCodec)),
      newID
    )
  }
}

case class SourceBuilder[T: Manifest] private (
  node: PairedProducer[T, Scalding, Storm],
  pairs: List[InjectionPair[_]],
  id: String,
  opts: Map[String, StormOptions] = Map.empty
) extends Serializable {
  import SourceBuilder.adjust

  def map[U: Manifest](fn: T => U): SourceBuilder[U] = copy(node = node.map(fn))
  def filter(fn: T => Boolean): SourceBuilder[T] = copy(node = node.filter(fn))
  def flatMap[U: Manifest](fn: T => TraversableOnce[U]): SourceBuilder[U] =
    copy(node = node.flatMap(fn))
  def flatMapBuilder[U: Manifest](newFlatMapper: FlatMapper[T, U]): SourceBuilder[U] =
    flatMap(newFlatMapper(_))

  def write[U](sink: CompoundSink[U])(conversion: T => TraversableOnce[U])(implicit batcher: Batcher, mf: Manifest[U]): SourceBuilder[T] = {
    val newNode =
      node.flatMap(conversion)
        .write(new BatchedSinkFromOffline[U](batcher, sink.offline), sink.online)
    copy(
      node = node.either(newNode)
        .flatMap {
        case Left(t) => Some(t)
        case Right(u) => None
      }
    )
  }

  def leftJoin[K, V, JoinedValue](service: CompoundService[K, JoinedValue])
    (implicit ev: T <:< (K, V), keyMf: Manifest[K], valMf: Manifest[V], joinedMf: Manifest[JoinedValue])
      : SourceBuilder[(K, (V, Option[JoinedValue]))] =
    copy(
      node = node.leftJoin(
        sys.error("TODO"), // https://github.com/twitter/summingbird/issues/68,
        StoreWrapper[K, JoinedValue](service.online)
      )
    )

  // Set the number of reducers used to shard out the EventSource
  // flatmapper in the offline flatmap step.
  def set(fms: FlatMapShards): SourceBuilder[T] =
    sys.error("TODO") // https://github.com/twitter/summingbird/issues/59

  // Set the cache size used in the online flatmap step.
  def set(size: CacheSize) = copy(opts = adjust(opts, id)(_.set(size)))
  def set(opt: FlatMapOption) = copy(opts = adjust(opts, id)(_.set(opt)))

  /**
    * Complete this builder instance with a BatchStore. At this point,
    * the Summingbird job can be executed on Hadoop.
    */
  def groupAndSumTo[K, V](store: ScaldingStore[K, V])(
    implicit ev: T <:< (K, V),
    env: Env,
    keyMf: Manifest[K],
    valMf: Manifest[V],
    keyCodec: Injection[K, Array[Byte]],
    valCodec: Injection[V, Array[Byte]],
    batcher: Batcher,
    monoid: Monoid[V],
    kord: Ordering[K]): CompletedBuilder[K, V] =
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
    monoid: Monoid[V],
    kord: Ordering[K]): CompletedBuilder[K, V] =
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
    monoid: Monoid[V],
    keyOrdering: Ordering[K]): CompletedBuilder[K, V] = {
    val newNode = node.sumByKey[K, V](
      store.offlineStore,
      MergeableStoreSupplier.from(store.onlineSupplier())
    )
    val cb = new CompletedBuilder(newNode, pairs, keyCodec, valCodec, SourceBuilder.freshUUID, opts)
    env.builder = cb
    cb
  }

  // useful when you need to merge two different Event sources
  def ++(other: SourceBuilder[T]): SourceBuilder[T] =
    copy(
      node = node.merge(other.node),
      pairs = pairs ++ other.pairs,
      id = SourceBuilder.freshUUID,
      opts = opts ++ other.opts
    )
}
