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
  * I tried to make Unzip2 an object with an apply method that took
  * all three type parameters, but ran into issues:

  [error] /Users/sritchie/code/twitter/summingbird/summingbird-core/src/main/scala/com/twitter/summingbird/PairedPlatform.scala:38: constructor of type com.twitter.summingbird.IdentityKeyedProducer[P,K,V] cannot be uniquely instantiated to expected type com.twitter.summingbird.Producer[com.twitter.summingbird.Platform2[P1,P2],T]
[error]  --- because ---
[error] undetermined type

  */
case class Unzip2[P1 <: Platform[P1], P2 <: Platform[P2]]() {
  def apply[T](root: Producer[Platform2[P1, P2], T])
      : (Producer[P1, T], Producer[P2, T]) =
    root match {
      case NamedProducer(producer, id) =>
        val (l, r) = apply(producer)
        (l.name(id), r.name(id))

      case IdentityKeyedProducer(producer) =>
        val (l, r) = apply(producer)
        (IdentityKeyedProducer(l), IdentityKeyedProducer(r))

      case Source(source, mf) =>
        val (leftSource, rightSource) = source
        (Source(leftSource, mf), Source(rightSource, mf))

      case OptionMappedProducer(producer, fn, mf) =>
        val (l, r) = apply(producer)
        (OptionMappedProducer(l, fn, mf), OptionMappedProducer(r, fn, mf))

      case FlatMappedProducer(producer, fn) =>
        val (l, r) = apply(producer)
        (l.flatMap(fn), r.flatMap(fn))

      case MergedProducer(l, r) =>
        val (ll, lr) = apply(l)
        val (rl, rr) = apply(r)
        (ll.merge(rl), lr.merge(rr))

      case AlsoProducer(l, r) =>
        val (ll, lr) = apply(l)
        val (rl, rr) = apply(r)
        (ll.also(rl), lr.also(rr))

      case WrittenProducer(producer, sink) =>
        val (l, r) = apply(producer)
        val (leftSink, rightSink) = sink
        (l.write(leftSink), r.write(rightSink))

      case LeftJoinedProducer(producer, service) =>
        val (l, r) = apply(producer)
        val (leftService, rightService) = service
        (l.leftJoin(leftService), r.leftJoin(rightService))

      case Summer(producer, store, monoid) =>
        val (l, r) = apply(producer)
        val (leftStore, rightStore) = store
        (Summer(l, leftStore, monoid), Summer(r, rightStore, monoid))
    }
}

/**
  * Platform capable of planning and executing two underlying
  * platforms in parallel.
  */
class Platform2[P1 <: Platform[P1], P2 <: Platform[P2]](p1: P1, p2: P2)
    extends Platform[Platform2[P1, P2]] {
  // The type of the inputs for this platform
  type Source[T] = (P1#Source[T], P2#Source[T])
  type Store[K, V] = (P1#Store[K, V], P2#Store[K, V])
  type Sink[T] = (P1#Sink[T], P2#Sink[T])
  type Service[K, V] = (P1#Service[K, V], P2#Service[K, V])
  type Plan[T] = (P1#Plan[T], P2#Plan[T])

  def plan[T](producer: Producer[Platform2[P1, P2], T]): Plan[T] = {
    val (leftProducer, rightProducer) = Unzip2[P1, P2]()(producer)
    (p1.plan(leftProducer), p2.plan(rightProducer))
  }
}
