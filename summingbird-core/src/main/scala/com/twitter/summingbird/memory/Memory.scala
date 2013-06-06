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

package com.twitter.summingbird.memory

import com.twitter.summingbird._
import batch.Batcher

trait MemoryStore[K, V] extends Store[Memory, K, V] {
  def put(pair: (K, V)): Unit
}

trait MemoryService[K, V] extends Service[Memory, K, V] {
  def get(k: K): Option[V]
}

object Memory {
  implicit def ser[T]: Serialization[Memory, T] = new Serialization[Memory, T] { }
  implicit def toSource[T](traversable: TraversableOnce[T])(implicit te: TimeExtractor[T]): Producer[Memory, T] =
    Producer.source[Memory, T](traversable)
}

class Memory extends Platform[Memory] {
  type Source[T] = TraversableOnce[T]

  def toIterator[T, K, V](producer: Producer[Memory, T]): Iterator[T] = {
    producer match {
      case NamedProducer(producer, _) => toIterator(producer)
      case IdentityKeyedProducer(producer) => toIterator(producer)
      case Source(source, _, _) => source.toIterator
      case OptionMappedProducer(producer, fn) => toIterator(producer).flatMap { fn(_).iterator }
      case FlatMappedProducer(producer, fn) => toIterator(producer).flatMap(fn)
      case MergedProducer(l, r) => toIterator(l) ++ toIterator(r)
      case LeftJoinedProducer(producer, svc) => {
        val service = svc.asInstanceOf[MemoryService[K, V]]
        toIterator(producer.asInstanceOf[Producer[Memory, (K, V)]]).map { case (k, v) =>
          (k, (v, service.get(k)))
        }
      }
      case Summer(producer, _, _, _, _, _) => toIterator(producer)
    }
  }

  def run[K, V](builder: Summer[Memory, K, V]): Unit = {
    val memStore = builder.store.asInstanceOf[MemoryStore[K, V]]
    toIterator(builder).foreach(memStore.put(_))
  }
}

object TestJobRunner {
  implicit val batcher = Batcher.ofHours(1)

  // This is dangerous, obviously.
  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)

  def testJob[P <: Platform[P]](source: Producer[P, Int], store: Store[P, Int, Int])
    (implicit ser: Serialization[P, Int]): Summer[P, Int, Int] =
    source
      .flatMap { x: Int => Some(x, x + 10) }
      .sumByKey(store)

  def runInMemory {
    def storeFn[K, V] = new MemoryStore[K, V] {
      def put(i: (K, V)) = println(i)
    }
    val mem = new Memory
    mem.run(testJob[Memory](List(1,2,3), storeFn))
  }
}
