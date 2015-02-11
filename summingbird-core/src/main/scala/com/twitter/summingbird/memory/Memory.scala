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
import com.twitter.summingbird.option.JobId
<<<<<<< HEAD
import com.twitter.summingbird.planner.DagOptimizer
=======
>>>>>>> master
import collection.mutable.{ Map => MutableMap }

object Memory {
  implicit def toSource[T](traversable: TraversableOnce[T])(implicit mf: Manifest[T]): Producer[Memory, T] =
    Producer.source[Memory, T](traversable)
}

<<<<<<< HEAD
trait MemoryService[-K, +V] {
  def get(k: K): Option[V]
}

=======
>>>>>>> master
class Memory(implicit jobID: JobId = JobId("default.memory.jobId")) extends Platform[Memory] {
  type Source[T] = TraversableOnce[T]
  type Store[K, V] = MutableMap[K, V]
  type Sink[-T] = (T => Unit)
  type Service[-K, +V] = MemoryService[K, V]
  type Plan[T] = Stream[T]

  private type Prod[T] = Producer[Memory, T]
  private type JamfMap = Map[Prod[_], Stream[_]]

<<<<<<< HEAD
  def counter(group: Group, name: Name): Option[Long] =
    MemoryStatProvider.getCountersForJob(jobID).flatMap { _.get(group.getString + "/" + name.getString).map { _.get } }
=======
  def counter(group: String, name: String): Option[Long] =
    MemoryStatProvider.getCountersForJob(jobID).flatMap { c =>
      c.get(group + "/" + name).map { _.get }
    }
>>>>>>> master

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
            (s.flatMap {
              case (k, v) =>
                fn(k).map((_, v))
            }, m)

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
              case (k, v) => (k, (v, service.get(k)))
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

  def plan[T](prod: TailProducer[Memory, T]): Stream[T] = {

<<<<<<< HEAD
    val registeredCounters: Seq[(Group, Name)] =
      JobCounters.getCountersForJob(jobID).getOrElse(Nil)
=======
    val registeredCounters: Seq[(String, String)] =
      Option(JobCounters.registeredCountersForJob.get(jobID)).map(_.toList).getOrElse(Nil)
>>>>>>> master

    if (!registeredCounters.isEmpty) {
      MemoryStatProvider.registerCounters(jobID, registeredCounters)
      SummingbirdRuntimeStats.addPlatformStatProvider(MemoryStatProvider)
    }
<<<<<<< HEAD

    val dagOptimizer = new DagOptimizer[Memory] {}
    val memoryTail = dagOptimizer.optimize(prod, dagOptimizer.ValueFlatMapToFlatMap)
    val memoryDag = memoryTail.asInstanceOf[TailProducer[Memory, T]]

    toStream(memoryDag, Map.empty)._1
=======
    toStream(prod, Map.empty)._1
>>>>>>> master
  }

  def run(iter: Stream[_]) {
    // Force the entire stream, taking care not to hold on to the
    // tail.
    iter.foreach(identity(_: Any))
  }
}
