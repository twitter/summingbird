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

package com.twitter.summingbird.typed

import java.util.Date

trait Service[P, K, V]
trait StreamSink[P, T]
trait Store[P, K, V] extends StreamSink[P, (K, V)]

sealed trait Producer[P, T]
case class Source[P, T, S](source: S) extends Producer[P, T]
case class FlatMappedProducer[P, T, U](producer: Producer[P, T], fn: T => TraversableOnce[U]) extends Producer[P, T]
case class MergedProducer[P, T](l: Producer[P, T], r: Producer[P, T]) extends Producer[P, T]
case class TeedProducer[P, T](l: Producer[P, T], r: StreamSink[P, T]) extends Producer[P, T]

trait KeyedProducer[P, K, V] extends Producer[P, (K, V)]
case class FnKeyedProducer[P, T, K, V](producer: Producer[P, T], fn: T => TraversableOnce[(K, V)])
case class LeftJoinedProducer[P, K, V, JoinedV](left: KeyedProducer[P, K, V], joined: Service[P, K, JoinedV])
    extends KeyedProducer[P, K, (V, Option[JoinedV])]

case class Completed[P, K, V](producer: KeyedProducer[P, K, V], store: Store[P, K, V])

trait Platform[P <: Platform[P]] {
  def merge[T](l: Producer[P, T], r: Producer[P, T]): Producer[P, T] =
    MergedProducer(l, r)

  def leftJoin[K, V, RightV](producer: KeyedProducer[P, K, V], service: Service[P, K, RightV])
      : KeyedProducer[P, K, (V, Option[RightV])] =
    LeftJoinedProducer(producer, service)

  def flatMap[T, U](producer: Producer[P, T])(fn: T => TraversableOnce[U]): Producer[P, U] =
    FlatMappedProducer(producer)(fn)

  def tee[T](producer: Producer[P, T], sink: StreamSink[P, T]): Producer[P, T] =
    TeedProducer(producer, sink)

  def sumByKey[K, V](producer: KeyedProducer[P, K, V], store: Store[P, K, V])
      : Completed[P, K, V] =
    Completed(producer, store)

  def run[K, V](builder: Completed[P, K, V]): Unit
}


trait MemoryStore[K, V] extends Store[MemP, K, V] {
  def put(pair: (K, V)): Unit
}

trait MemoryStreamSink[K, T] extends StreamSink[MemP, T] {
  def put(t: T): Unit
}

trait MemoryService[K, V] extends Service[MemP, K, V] {
  def get(k: K): Option[V]
}

class MemP extends Platform[MemP] {
  def toIterator[T](producer: Producer[MemP, T]): Iterator[T] = {
    producer match {
      case Source(source) => source.asInstanceOf[Iterator[T]]
      case FlatMappedProducer(producer, fn) => toIterator(producer).flatMap(fn)
      case MergedProducer(l, r) => toIterator(l) ++ toIterator(r)
      case TeedProducer(l, streamSink) => {
        toIterator(l).map { t =>
          streamSink.put(t)
          t
        }
      }
      case FnKeyedProducer(producer, fn) => toIterator(producer).flatMap(fn)
      case LeftJoinedProducer(producer, service) => {
        val service = service.asInstanceOf[MemoryService[_, _]]
        toIterator(producer).map { case (k, v) =>
          (k -> v, service.get(v))
        }
      }

    }
  }

  def run[K, V](builder: Completed[MemP, K, V]): Unit = {
    val Completed(producer, store) = builder
    val memStore = store.asInstanceOf[MemoryStore[K, V]]
      (producer match {
      case LeftJoinedProducer[MemP, K, V, JoinedV](producer, service) => {
        val service = service.asInstanceOf[MemoryService[K, JoinedV, Some]]
        toIterator(producer).map { case (k, v) =>
          (k -> v, unwrap(service.get(v)))
        }
      }
      case FnKeyedProducer[MemP, T, K, V](producer, fn) =>
          toIterator(producer).flatMap(fn)
    }).foreach(memStore.put(_))
  }
}

// Storm or Scalding right now.

trait TimeExtractor[T] extends (T => Date)

// Nodes in our AST: source, flatMapped, scannedSum, service.

/**
trait Source[P <: Platform[P], T] {
  def flatMap[K, V](fn: T => TraversableOnce[(K, V)])(implicit platform: P) =
    platform.flatMap(this)(fn)
}

class StormSource[T](spout: ScalaSpout[T]) extends Source[Storm, T] {
    // Here are the planning methods:
  def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String): String = {
    val spoutName = "spout" + suffix
    val scalaSpout = eventSource.spout.get
    val filteredSpout = scalaSpout
      .getSpout { scheme =>
        predOption
          .map { scheme filter _ }
          .getOrElse { scheme }
          .map { e => (e, timeOf(e)) }
    }
    tb.setSpout(spoutName, filteredSpout, scalaSpout.parallelism)
    spoutName
  }
}

class SingleFMB[T, K, V](val source: Source[Storm, T])
  (val fn: T => TraversableOnce[(K, V)]) extends FlatMappedBuilder[Storm, K, V] {

  def flatMapName(suffix: String) = "flatMap" + suffix

  override def addToTopo(env: StormEnv, tb: TopologyBuilder, suffix: String) {
    val spoutName = sourceBuilder.addToTopo(env, tb, suffix)
    // The monoid here must be of the right type or we are screwed anyway, the builder
    // ensures this:
    implicit val monoid: Monoid[Value] = env.builder.monoid.asInstanceOf[Monoid[Value]]
    implicit val batcher: Batcher = env.builder.batcher
    tb.setBolt(flatMapName(suffix),
               new FlatMapBolt(stormFm, flatMapCacheSize, stormMetrics),
               flatMapParallelism.parHint)
      .shuffleGrouping(spoutName)
  }

  override def attach(groupBySumBolt: BoltDeclarer, suffix: String) =
    groupBySumBolt.fieldsGrouping(flatMapName(suffix), new Fields(AGG_KEY))
}

class Storm extends Platform[Storm] {
  def semigroup: Semigroup[FlatMappedBuilder[Storm, K, V]]

  def flatMap[T, K, V](source: Source[Storm, T])
    (fn: T => TraversableOnce[(K, V)]): FlatMappedBuilder[Storm, T, K, V] =

  def leftJoin[RightV](fmb: FlatMappedBuilder[Storm, K, V], service: Service[Storm, K, RightV])
      : FlatMappedBuilder[Storm, K, (V, Option[RightV])]

  def sumByKey[K, V](fmb: FlatMappedBuilder[Storm, K, V], store: Store[Storm, K, V])
      : CompletedBuilder[Storm, K, V]

  def run(builder: CompletedBuilder[Storm, K, V]): Unit
}

trait AST[P <: Platform[P]] {
}

object SomeJobs {
  def adserverJob[V](samplePercent: Double)(fn: ImpressionEvent => V): Builder[FeatureKey, V] =
    impressionEventSource
      .filter(_ => scala.util.Random.nextDouble < samplePercent)
      .flatMap(t => List((k, v)))

  def firstPart = source.map { (_.getUserId, 1L) }

  def tweetJob[P, K, V](fn: (String, Long) => (K, V)): CompletedBuilder[P, K, V] =
    firstPart
      .map(fn)
      .groupAndSumTo(store)

}

object Source {
  def read[T, P](implicit source: Source[T, P]): Source[T, P] = source
}

class TypedJob[P <: Platform[P]](implicit source: Source[Tweet, P], ast: AST[P], platform: P) {
  Source.read[Tweet, P]
    .map { (_.getUserId, 1L) }
    .groupAndSumTo(store[String, Long])
}

class StormRunner(args: Args) {
  import TwitterSources._
  implicit val storm: Platform[Storm] = Storm
  implicit val ast: AST[P] = new AST[Storm]()
  val job = new TypedJob[Storm]
  storm.run(ast)
}
  */
