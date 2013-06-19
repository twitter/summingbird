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

import com.twitter.algebird.Monoid
import com.twitter.summingbird._
import collection.mutable.{ Map => MutableMap }

object Memory {
  implicit def toSource[T](traversable: TraversableOnce[T])(implicit te: TimeExtractor[T], mf: Manifest[T]): Producer[Memory, T] =
    Producer.source[Memory, T](traversable)
}

class Memory extends Platform[Memory] {
  type Source[T] = TraversableOnce[T]
  type Store[K, V] = MutableMap[K, V]
  type Service[-K, +V] = (K => Option[V])
  type Plan[T] = Iterator[T]

  def toIterator[T, K, V](producer: Producer[Memory, T]): Iterator[T] = {
    producer match {
      case NamedProducer(producer, _) => toIterator(producer)
      case IdentityKeyedProducer(producer) => toIterator(producer)
      case Source(source, _, _) => source.toIterator
      case OptionMappedProducer(producer, fn, mf) => toIterator(producer).flatMap { fn(_).iterator }
      case FlatMappedProducer(producer, fn) => toIterator(producer).flatMap(fn)
      case MergedProducer(l, r) => toIterator(l) ++ toIterator(r)
      case LeftJoinedProducer(producer, service) =>
        toIterator(producer).map { case (k, v) =>
          (k, (v, service(k)))
        }
      case Summer(producer, store, monoid) =>
        toIterator(producer).map { case pair@(k, deltaV) =>
          val newV = store.get(k)
            .map { monoid.plus(_, deltaV) }
            .getOrElse(deltaV)
          store.update(k, newV)
          pair
        }
    }
  }
  def plan[T](prod: Producer[Memory, T]): Iterator[T] =
    toIterator(prod)

  def run(iter: Iterator[_]) {
    iter.foreach { it => it /* just go through the whole thing */ }
  }
}
