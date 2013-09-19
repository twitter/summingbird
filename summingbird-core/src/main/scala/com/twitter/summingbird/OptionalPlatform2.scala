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

package com.twitter.summingbird

/**
  * @author Aaron Siegel
  */
case class OptionalUnzip2[P1 <: Platform[P1], P2 <: Platform[P2]]() {
  def apply[T](root: Producer[OptionalPlatform2[P1, P2], T])
      : (Option[Producer[P1, T]], Option[Producer[P2, T]]) =
    root match {
      case NamedProducer(producer, id) =>
        val (l, r) = apply(producer)
        (l.map(_.name(id)), r.map(_.name(id)))

      case IdentityKeyedProducer(producer) =>
        val (l, r) = apply(producer)
        (l.map(IdentityKeyedProducer(_)), r.map(IdentityKeyedProducer(_)))

      case Source(source, mf) =>
        val (leftSource, rightSource) = source
        (leftSource.map(Source(_, mf)), rightSource.map(Source(_, mf)))

      case OptionMappedProducer(producer, fn, mf) =>
        val (l, r) = apply(producer)
        (l.map(OptionMappedProducer(_, fn, mf)), r.map(OptionMappedProducer(_, fn, mf)))

      case FlatMappedProducer(producer, fn) =>
        val (l, r) = apply(producer)
        (l.map(_.flatMap(fn)), r.map(_.flatMap(fn)))

      case MergedProducer(l, r) =>
        val (ll, lr) = apply(l)
        val (rl, rr) = apply(r)
        val mergedl = for (lli <- ll; rli <- rl) yield lli.merge(rli)
        val mergedr = for (lri <- lr; rri <- rr) yield lri.merge(rri)
        (mergedl, mergedr)

      case AlsoProducer(l, r) =>
        val (ll, lr) = apply(l)
        val (rl, rr) = apply(r)
        val alsol = for (lli <- ll; rli <- rl) yield lli.also(rli)
        val alsor = for (lri <- lr; rri <- rr) yield lri.also(rri)
        (alsol, alsor)

      case WrittenProducer(producer, sink) =>
        val (l, r) = apply(producer)
        val (leftSink, rightSink) = sink
        val sinkl = for (li <- l; leftSinki <- leftSink) yield li.write(leftSinki)
        val sinkr = for (ri <- r; rightSinki <- rightSink) yield ri.write(rightSinki)
        (sinkl, sinkr)

      case LeftJoinedProducer(producer, service) =>
        val (l, r) = apply(producer)
        val (leftService, rightService) = service
        val left = for (li <- l; leftServicei <- leftService) yield li.leftJoin(leftServicei)
        val right = for (ri <- r; rightServicei <- rightService) yield ri.leftJoin(rightServicei)
        (left, right)

      case Summer(producer, store, monoid) =>
        val (l, r) = apply(producer)
        val (leftStore, rightStore) = store
        val left = for (li <- l; leftStorei <- leftStore) yield Summer(li, leftStorei, monoid)
        val right = for (ri <- r; rightStorei <- rightStore) yield Summer(ri, rightStorei, monoid)
        (left, right)
    }
}

/**
  * Platform capable of planning and executing at most 2 underlying
  * platforms in parallel.
  */
class OptionalPlatform2[P1 <: Platform[P1], P2 <: Platform[P2]](p1: P1, p2: P2)
    extends Platform[OptionalPlatform2[P1, P2]] {
  // The type of the inputs for this platform
  type Source[T] = (Option[P1#Source[T]], Option[P2#Source[T]])
  type Store[K, V] = (Option[P1#Store[K, V]], Option[P2#Store[K, V]])
  type Sink[T] = (Option[P1#Sink[T]], Option[P2#Sink[T]])
  type Service[K, V] = (Option[P1#Service[K, V]], Option[P2#Service[K, V]])
  type Plan[T] = (Option[P1#Plan[T]], Option[P2#Plan[T]])

  override def plan[T](producer: Producer[OptionalPlatform2[P1, P2], T]): Plan[T] = {
    val (leftProducer, rightProducer) = OptionalUnzip2[P1, P2]()(producer)
    (leftProducer.map(p1.plan(_)), rightProducer.map(p2.plan(_)))
  }
}
