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
import collection.mutable.{ Map => MutableMap, Queue => MQueue }

object Memory {
  implicit def toSource[T](traversable: TraversableOnce[T])(implicit mf: Manifest[T]): Producer[Memory, T] =
    Producer.source[Memory, T](traversable)
}

case class MemoryWindow[K, V](size: Int) {
  private val queue = new MQueue[(K,V)]()
  private val memory = MutableMap[K,V]()
  def put(kv: (K,V)): Unit = {
    queue += kv
    memory += kv
    if(queue.size > size) {
      val (k, _) = queue.dequeue
      memory -= k
    }
  }
  def get(k: K): Option[V] = memory.get(k)
}

class Memory extends Platform[Memory] {
  type Source[T] = TraversableOnce[T]
  type Store[K, V] = MutableMap[K, V]
  type Sink[-T] = (T => Unit)
  type Service[-K, +V] = (K => Option[V])
  type Plan[T] = Stream[T]

  type Window[K, V] = MemoryWindow[K, V]

  private type Prod[T] = Producer[Memory, T]
  private type JamfMap = Map[Prod[_], Stream[_]]

  def toStream[T, K, V](outerProducer: Prod[T], jamfs: JamfMap): (Stream[T], JamfMap) =
    jamfs.get(outerProducer) match {
      case Some(s) => (s.asInstanceOf[Stream[T]], jamfs)
      case None =>
        val (s, m) = outerProducer match {
          case NamedProducer(producer, _) => toStream(producer, jamfs)
          case IdentityKeyedProducer(producer) => toStream(producer, jamfs)
          case Source(source, _) => (source.toStream, jamfs)
          case OptionMappedProducer(producer, fn, mf) =>
            val (s, m) = toStream(producer, jamfs)
            (s.flatMap(fn(_)), m)

          case FlatMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.flatMap(fn(_)), m)

          case MergedProducer(l, r) =>
            val (leftS, leftM) = toStream(l, jamfs)
            val (rightS, rightM) = toStream(r, leftM)
            // TODO: this is not what anyone would expect:
            // should interleave`
            (leftS ++ rightS, rightM)

          case WrittenProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.map { i => fn(i); i }, m)

          case WindowJoinedProducer(left, right, window) =>
            val (leftS, leftM) = toStream(left, jamfs)
            val (rightS, rightM) = toStream(right, leftM)
            val rightIt = rightS.iterator
            val joined = leftS.map { case (k, v) =>
              val res = (k, (v, window.get(k)))
              // Feed the next right:
              if(rightIt.hasNext) {
                window.put(rightIt.next)
              }
              res
            }
            (joined, rightM)

          case LeftJoinedProducer(producer, service) =>
            val (s, m) = toStream(producer, jamfs)
            val joined = s.map {
              case (k, v) => (k, (v, service(k)))
            }
            (joined, m)

          case Summer(producer, store, monoid) =>
            val (s, m) = toStream(producer, jamfs)
            val summed = s.map {
              case pair @ (k, deltaV) =>
                val newV = store.get(k)
                  .map { monoid.plus(_, deltaV) }
                  .getOrElse(deltaV)
                store.update(k, newV)
                pair
            }
            (summed, m)
        }
        (s.asInstanceOf[Stream[T]], m + (outerProducer -> s))
    }

  def plan[T](prod: Producer[Memory, T]): Stream[T] =
    toStream(prod, Map.empty)._1

  def run(iter: Stream[_]) {
    // Force the entire stream, taking care not to hold on to the
    // tail.
    iter.foreach(identity(_: Any))
  }
}
