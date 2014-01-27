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
import com.twitter.summingbird.option.StatList

import collection.mutable.{ Map => MutableMap }

object Memory {
  implicit def toSource[T](traversable: TraversableOnce[T])(implicit mf: Manifest[T]): Producer[Memory, T] =
    Producer.source[Memory, T](traversable)
}

class Memory(options: Map[String, Options] = Map[String, Options]()) extends Platform[Memory] {
  type Source[T] = TraversableOnce[T]
  type Store[K, V] = MutableMap[K, V]
  type Sink[-T] = (T => Unit)
  type Service[-K, +V] = (K => Option[V])
  type Plan[T] = Stream[T]

  private type Prod[T] = Producer[Memory, T]
  private type JamfMap = Map[Prod[_], Stream[_]]

  def toStream[T, K, V](outerProducer: Prod[T], jamfs: JamfMap): (Stream[T], JamfMap) =
    jamfs.get(outerProducer) match {
      case Some(s) => (s.asInstanceOf[Stream[T]], jamfs)
      case None =>
        val (s, m) = outerProducer match {
          case NamedProducer(producer, _) => toStream(producer, jamfs)
          case IdentityKeyedProducer(producer) => toStream(producer, jamfs)
          case Source(source) => (source.toStream, jamfs)
          case OptionMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.flatMap(fn(_)), m)

          case FlatMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.flatMap(fn(_)), m)

          case MergedProducer(l, r) =>
            val (leftS, leftM) = toStream(l, jamfs)
            val (rightS, rightM) = toStream(r, leftM)
            (leftS ++ rightS, rightM)

          case KeyFlatMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.flatMap{
              case (k, v) =>
                fn(k).map((_, v))
             }
            , m)

          case AlsoProducer(l, r) =>
            //Plan the first one, but ignore it
            val (left, leftM) = toStream(l, jamfs)
            // We need to force all of left to make sure any
            // side effects in write happen
            val lforcedEmpty = left.filter(_ => false)
            val (right, rightM) = toStream(r, leftM)
            (right ++ lforcedEmpty, rightM)

          case WrittenProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.map { i => fn(i); i }, m)

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
                val oldV = store.get(k)
                val newV = oldV.map { monoid.plus(_, deltaV) }
                  .getOrElse(deltaV)
                store.update(k, newV)
                (k, (oldV, deltaV))
            }
            (summed, m)
        }
        (s.asInstanceOf[Stream[T]], m + (outerProducer -> s))
    }

  private def setUpStats {
    val statClasses = options.values.flatMap{ opts =>
      opts.get[StatList].map(_.get).getOrElse(List[Stats]())
    }

    statClasses.map(_.uniqueId).toSet.foreach { id: String =>
      Stats.addIncrementor(id, MemoryStats.getIncrementor(id))
    }
  }

  def plan[T](prod: TailProducer[Memory, T]): Stream[T] = {
    setUpStats
    toStream(prod, Map.empty)._1
  }

  def run(iter: Stream[_]) {
    // Force the entire stream, taking care not to hold on to the
    // tail.
    iter.foreach(identity(_: Any))
  }
}
