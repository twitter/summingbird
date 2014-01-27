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

import com.twitter.algebird.MapAlgebra
import com.twitter.algebird.{Monoid, Semigroup}
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

/**
  * Helpful functions and test graphs designed to flex Summingbird
  * planners.
  */

object TestGraphs {
  def diamondJobInScala[T, K, V: Monoid]
    (source: TraversableOnce[T])
    (fnA: T => TraversableOnce[(K, V)])
    (fnB: T => TraversableOnce[(K, V)]): Map[K, V] = {
    val stream = source.toStream
    val left = stream.flatMap(fnA)
    val right = stream.flatMap(fnB)
    MapAlgebra.sumByKey(left ++ right)
  }

  def diamondJob[P <: Platform[P], T, K, V: Monoid]
    (source: Producer[P, T], sink: P#Sink[T], store: P#Store[K, V])
    (fnA: T => TraversableOnce[(K, V)])
    (fnB: T => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] = {
    val written = source.write(sink)
    val left = written.flatMap(fnA)
    val right = written.flatMap(fnB)
    left.merge(right).sumByKey(store)
  }

  def singleStepInScala[T, K, V: Monoid]
    (source: TraversableOnce[T])
    (fn: T => TraversableOnce[(K, V)]): Map[K, V] =
    MapAlgebra.sumByKey(
      source.flatMap(fn)
    )

  def singleStepJob[P <: Platform[P], T, K, V: Monoid]
    (source: Producer[P, T], store: P#Store[K, V])
    (fn: T => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] =
    source
      .flatMap(fn).name("FM")
      .sumByKey(store)

  def twinStepOptionMapFlatMapScala[T1, T2, K, V: Monoid]
    (source: TraversableOnce[T1])
    (fnA: T1 => Option[T2], fnB: T2 => TraversableOnce[(K, V)]): Map[K, V] =
    MapAlgebra.sumByKey(
      source.flatMap(fnA(_).iterator).flatMap(fnB)
    )

  def twinStepOptionMapFlatMapJob[P <: Platform[P], T1, T2, K, V: Monoid]
    (source: Producer[P, T1], store: P#Store[K, V])
    (fnA: T1 => Option[T2], fnB: T2 => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] =
    source
      .optionMap(fnA)
      .flatMap(fnB)
      .sumByKey(store)

  def singleStepMapKeysInScala[T, K1, K2, V: Monoid]
    (source: TraversableOnce[T])
    (fnA: T => TraversableOnce[(K1, V)], fnB: K1 => TraversableOnce[K2]): Map[K2, V] =
    MapAlgebra.sumByKey(
      source.flatMap(fnA).flatMap{x => fnB(x._1).map((_, x._2))}
    )

  def singleStepMapKeysJob[P <: Platform[P], T, K1, K2, V: Monoid]
    (source: Producer[P, T], store: P#Store[K2, V])
    (fnA: T => TraversableOnce[(K1, V)], fnB: K1 => TraversableOnce[K2]): TailProducer[P, (K2, (Option[V], V))] =
    source
      .flatMap(fnA)
      .flatMapKeys(fnB)
      .sumByKey(store)

  def singleStepMapKeysStatJob[P <: Platform[P], T, K, V: Monoid]
    (source: Producer[P, T], store: P#Store[K, V])
    (fnA: T => TraversableOnce[(K, V)]): (List[Stats], TailProducer[P, (K, (Option[V], V))]) = {
      val myStat = Stats("myCustomStat")
      val tail = source.map {v =>
        myStat.incr
        v
      }.flatMap(fnA)
      .sumByKey(store)
      (List(myStat), tail)
    }

  def leftJoinInScala[T, U, JoinedU, K, V: Monoid]
    (source: TraversableOnce[T])
    (service: K => Option[JoinedU])
    (preJoinFn: T => TraversableOnce[(K, U)])
    (postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): Map[K, V] =
    MapAlgebra.sumByKey(
      source
        .flatMap(preJoinFn)
        .map { case (k, v) => (k, (v, service(k))) }
        .flatMap(postJoinFn)
    )

  def leftJoinJob[P <: Platform[P], T, U, JoinedU, K, V: Monoid](
    source: Producer[P, T],
    service: P#Service[K, JoinedU],
    store: P#Store[K, V])
    (preJoinFn: T => TraversableOnce[(K, U)])
    (postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] =
    source
      .name("My named source")
      .flatMap(preJoinFn)
      .leftJoin(service)
      .name("My named flatmap")
      .flatMap(postJoinFn)
      .sumByKey(store)

  def realJoinTestJob[P <: Platform[P], T1, T2, T3, T4, K1, K2, U, JoinedU, V: Monoid] (
    source1: Producer[P, T1],
    source2: Producer[P, T2],
    source3: Producer[P, T3],
    source4: Producer[P, T4],
    service: P#Service[K1, JoinedU],
    store: P#Store[K2, V],
    simpleFM1: T1 => TraversableOnce[(K2, V)],
    simpleFM2: T2 => TraversableOnce[(K2, V)],
    simpleFM3: T3 => TraversableOnce[(K2, V)],
    preJoin: T4 => (K1, U),
    postJoin: ((K1, (U, Option[JoinedU]))) => TraversableOnce[(K2, V)]
    ): TailProducer[P, (K2, (Option[V], V))] = {
    val data1 = source1.flatMap(simpleFM1)
    val data2 = source2.flatMap(simpleFM2)
    val data3 = source3.flatMap(simpleFM3)
    val data4 = source4.map(preJoin).leftJoin(service).flatMap(postJoin)
    data1.merge(data2).merge(data3).merge(data4).sumByKey(store).name("Customer Supplied Job")
  }

  def writtenPostSum[P <: Platform[P], T, K, V: Monoid]
    (source: Producer[P, T], sink: P#Sink[(K, (Option[V], V))], store: P#Store[K, V])
    (fnA: T => TraversableOnce[(K, V)])
    : TailProducer[P, (K, (Option[V], V))] = {
    val left = source.flatMap(fnA)
    left.sumByKey(store).write(sink)
  }

 def realJoinTestJobInScala[P <: Platform[P], T1, T2, T3, T4, K1, K2, U, JoinedU, V: Monoid] (
    source1: List[T1],
    source2: List[T2],
    source3: List[T3],
    source4: List[T4],
    service: K1 => Option[JoinedU],
    simpleFM1: T1 => TraversableOnce[(K2, V)],
    simpleFM2: T2 => TraversableOnce[(K2, V)],
    simpleFM3: T3 => TraversableOnce[(K2, V)],
    preJoin: T4 => (K1, U),
    postJoin: ((K1, (U, Option[JoinedU]))) => TraversableOnce[(K2, V)]
    ): Map[K2, V] = {
    val data1 = source1.flatMap(simpleFM1)
    val data2 = source2.flatMap(simpleFM2)
    val data3 = source3.flatMap(simpleFM3)
    val data4 = source4.map(preJoin).map { case (k, v) => (k, (v, service(k))) }
                .flatMap(postJoin)
    MapAlgebra.sumByKey(
      data1 ::: data2 ::: data3 ::: data4
    )
 }


  def multipleSummerJobInScala[T1, T2, K1, V1: Monoid, K2, V2: Monoid]
    (source: List[T1])
    (fnR: T1 => TraversableOnce[T2], fnA: T2 => TraversableOnce[(K1, V1)], fnB: T2 => TraversableOnce[(K2, V2)])
    : (Map[K1, V1], Map[K2, V2]) = {
      val mapA = MapAlgebra.sumByKey(source.flatMap(fnR).flatMap(fnA))
      val mapB = MapAlgebra.sumByKey(source.flatMap(fnR).flatMap(fnB))
      (mapA, mapB)
    }

  def multipleSummerJob[P <: Platform[P], T1, T2, K1, V1: Monoid, K2, V2: Monoid]
      (source: Producer[P, T1], store1: P#Store[K1, V1], store2: P#Store[K2, V2])
    (fnR: T1 => TraversableOnce[T2], fnA: T2 => TraversableOnce[(K1, V1)],
      fnB: T2 => TraversableOnce[(K2, V2)]): TailProducer[P, (K2, (Option[V2], V2))] = {
      val combined = source.flatMap(fnR)
      val calculated = combined.flatMap(fnB).sumByKey(store2)
      combined.flatMap(fnA).sumByKey(store1).also(calculated)
    }

  def mapOnlyJob[P <: Platform[P], T, U](
    source: Producer[P, T],
    sink: P#Sink[U]
    )(mapOp: T => TraversableOnce[U]): TailProducer[P, U] =
    source
      .flatMap(mapOp)
      .write(sink)

  def lookupJob[P <: Platform[P], T, U](
    source: Producer[P, T], srv: P#Service[T, U], sink: P#Sink[(T, U)]): TailProducer[P, (T, U)] =
      source.lookup(srv).collectValues { case Some(v) => v }.write(sink)

  def lookupJobInScala[T, U](in: List[T], srv: (T) => Option[U]): List[(T, U)] =
    in.map { t => (t, srv(t)) }.collect { case (t, Some(u)) => (t,u) }

  def twoSumByKey[P <: Platform[P], K, V: Monoid, K2](
    source: Producer[P, (K, V)],
    store: P#Store[K, V],
    fn: K => List[K2],
    store2: P#Store[K2, V]): TailProducer[P, (K2, (Option[V], V))] =
      source
        .sumByKey(store)
        .mapValues(_._2)
        .flatMapKeys(fn)
        .sumByKey(store2)

  def scanSum[V: Semigroup](it: Iterator[V]): Iterator[(Option[V], V)] = {
    var prev: Option[V] = None
    it.map { v =>
      val res = (prev, v)
      prev = prev.map(Semigroup.plus(_, v)).orElse(Some(v))
      res
    }
  }

  def twoSumByKeyInScala[K1, V: Semigroup, K2](in: List[(K1, V)], fn: K1 => List[K2]): (Map[K1, V], Map[K2, V]) = {
    val sum1 = MapAlgebra.sumByKey(in)
    val sumStream = in.groupBy(_._1)
      .mapValues { l => scanSum(l.iterator.map(_._2)).toList }
      .toIterable
      .flatMap { case (k, lv) => lv.map((k, _)) }
    val v2 = sumStream.map { case (k, (_, v)) => fn(k).map { (_, v) } }.flatten
    val sum2 = MapAlgebra.sumByKey(v2)
    (sum1, sum2)
  }
}

class TestGraphs[P <: Platform[P], T: Manifest: Arbitrary, K: Arbitrary, V: Arbitrary: Equiv: Monoid](platform: P)(
  store: () => P#Store[K, V])(sink: () => P#Sink[T])(
  sourceMaker: TraversableOnce[T] => Producer[P, T])(
  toLookupFn: P#Store[K, V] => (K => Option[V]))(
  toSinkChecker: (P#Sink[T], List[T]) => Boolean)(
  run: (P, P#Plan[_]) => Unit){

  def diamondChecker(items: List[T], fnA: T => List[(K, V)], fnB: T => List[(K, V)]): Boolean = {
    val currentStore = store()
    val currentSink = sink()
    // Use the supplied platform to execute the source into the
    // supplied store.
    val plan = platform.plan {
      TestGraphs.diamondJob(sourceMaker(items), currentSink, currentStore)(fnA)(fnB)
    }
    run(platform, plan)
    val lookupFn = toLookupFn(currentStore)
    TestGraphs.diamondJobInScala(items)(fnA)(fnB).forall { case (k, v) =>
      val lv = lookupFn(k).getOrElse(Monoid.zero)
      Equiv[V].equiv(v, lv)
    } && toSinkChecker(currentSink, items)
  }

    /**
    * Accepts a platform, and supplier of an EMPTY store from K -> V
    * and returns a ScalaCheck property. The property generates a
    * random function from T => TraversableOnce[(K, V)], wires up
    * singleStepJob summingbird job using this function and runs the
    * job using the supplied platform.
    *
    * Results are retrieved using the supplied toMap function. The
    * initial data source is generated using the supplied sourceMaker
    * function.
      */
  def singleStepChecker(items: List[T], fn: T => List[(K, V)]): Boolean = {
    val currentStore = store()
    // Use the supplied platform to execute the source into the
    // supplied store.
    val plan = platform.plan {
      TestGraphs.singleStepJob(sourceMaker(items), currentStore)(fn)
    }
    run(platform, plan)
    val lookupFn = toLookupFn(currentStore)
    TestGraphs.singleStepInScala(items)(fn).forall { case (k, v) =>
      val lv = lookupFn(k).getOrElse(Monoid.zero)
      Equiv[V].equiv(v, lv)
    }
  }

  /**
    * Accepts a platform, a service of K -> JoinedU and a supplier of
    * an EMPTY store from K -> V and returns a ScalaCheck
    * property. The property generates random functions between the
    * types required by leftJoinJob, wires up a summingbird job using
    * those functions and runs the job using the supplied platform.
    *
    * Results are retrieved using the supplied toMap function. The
    * service is tested in scala's in-memory mode using serviceToFn,
    * and the initial data source is generated using the supplied
    * sourceMaker function.
    */
  def leftJoinChecker[U: Arbitrary, JoinedU: Arbitrary](service: P#Service[K, JoinedU], serviceToFn: P#Service[K, JoinedU] => (K => Option[JoinedU]),
    items: List[T], preJoinFn: T => List[(K, U)],
    postJoinFn: ((K, (U, Option[JoinedU]))) => List[(K, V)]): Boolean = {
      val currentStore = store()

      val plan = platform.plan {
        TestGraphs.leftJoinJob(sourceMaker(items), service, currentStore)(preJoinFn)(postJoinFn)
      }
      run(platform, plan)
      val serviceFn = serviceToFn(service)
      val lookupFn = toLookupFn(currentStore)

      MapAlgebra.sumByKey(
        items
          .flatMap(preJoinFn)
          .map { case (k, u) => (k, (u, serviceFn(k))) }
          .flatMap(postJoinFn)
      ).forall { case (k, v) =>
          val lv = lookupFn(k).getOrElse(Monoid.zero)
          Equiv[V].equiv(v, lv)
      }
    }
}
