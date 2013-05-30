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

import com.twitter.summingbird.batch.Batcher

trait MemoryStore[K, V] extends Store[MemP, K, V] {
  def put(pair: (K, V)): Unit
}

trait MemoryStreamSink[K, T] extends StreamSink[MemP, T] {
  def put(t: T): Unit
}

trait MemoryService[K, V] extends Service[MemP, K, V] {
  def get(k: K): Option[V]
}

object MemP {
  implicit def ser[T]: Serialization[MemP, T] = new Serialization[MemP, T] { }
  implicit def toSource[T](iterator: Iterator[T]): Producer[MemP, T] =
    Producer.source[MemP, T, Iterator[T]](iterator)
}

class MemP extends Platform[MemP] {
  def toIterator[T, K, V](producer: Producer[MemP, T]): Iterator[T] = {
    producer match {
      case NamedProducer(producer, _) => toIterator(producer)
      case IdentityKeyedProducer(producer) => toIterator(producer)
      case Source(source, _, _) => source.asInstanceOf[Iterator[T]]
      case FlatMappedProducer(producer, fn) => toIterator(producer).flatMap(fn)
      case MergedProducer(l, r) => toIterator(l) ++ toIterator(r)
      case TeedProducer(l, streamSink) => {
        toIterator(l).map { t =>
          streamSink.asInstanceOf[MemoryStreamSink[MemP, T]].put(t)
          t
        }
      }
      case LeftJoinedProducer(producer, svc) => {
        val service = svc.asInstanceOf[MemoryService[K, V]]
        toIterator(producer.asInstanceOf[Producer[MemP, (K, V)]]).map { case (k, v) =>
          (k, (v, service.get(k)))
        }
      }
    }
  }

  def run[K, V](builder: Completed[MemP, K, V]): Unit = {
    val memStore = builder.store.asInstanceOf[MemoryStore[K, V]]
    toIterator(builder.producer).foreach(memStore.put(_))
  }
}

object TestJobRunner {
  implicit val batcher = Batcher.ofHours(1)

  def testJob[P <: Platform[P]](
    source: Producer[P, Int],
    store: Store[P, Int, Int])
    (implicit ser: Serialization[P, Int]): Completed[P, Int, Int] =
    source
      .flatMap { x: Int => Some(x, x + 10) }
      .sumByKey(store)

  def runInMemory {
    def storeFn[K, V] = new MemoryStore[K, V] {
      def put(i: (K, V)) = println(i)
    }
    val mem = new MemP
    mem.run(testJob[MemP](Iterator(1,2,3), storeFn))
  }
}
