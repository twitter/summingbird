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

import com.twitter.algebird.{ MapAlgebra, Monoid, Semigroup }
import com.twitter.summingbird.option.JobId
import org.scalacheck.Arbitrary

/**
 * Helpful functions and test graphs designed to flex Summingbird
 * planners.
 */

object TestGraphs {
  // implicit ordering on the either pair
  implicit def eitherOrd[T, U]: Ordering[Either[T, U]] =
    new Ordering[Either[T, U]] {
      def compare(l: Either[T, U], r: Either[T, U]) =
        (l, r) match {
          case (Left(_), Right(_)) => -1
          case (Right(_), Left(_)) => 1
          case (Left(_), Left(_)) => 0
          case (Right(_), Right(_)) => 0
        }
    }

  // Helpers

  private def sum[V: Semigroup](opt: Option[V], v: V): V = if (opt.isDefined) Semigroup.plus(opt.get, v) else v

  private def scanSum[V: Semigroup](it: Iterator[V]): Iterator[(Option[V], V)] = {
    var prev: Option[V] = None
    it.map { v =>
      val res = (prev, v)
      prev = Some(sum(prev, v))
      res
    }
  }

  /**
   * This function simulates the loop join in ScaldingPlatform loopJoin. Used when joining against a store and the store depends on the result of the join.
   * The function takes an Iterable of Either and a valuesFn function. The Eithers in the Iterable are updates to the store, corresponding to the two TypedPipes
   * in ScaldingPlatform loopJoin (summingbird-scalding/src/main/scala/com/twitter/summingbird/scalding/ScaldingPlatform.scala).
   * The result is a join stream and the output stream of the store.
   */
  private def loopJoinInScala[K: Ordering, U, V: Monoid](leftAndRight: Iterable[(K, (Long, Either[U, V]))], valuesFn: ((Long, (U, Option[V]))) => TraversableOnce[(Long, V)]): List[(K, List[(Option[(Long, (U, Option[V]))], Option[(Long, (Option[V], V))])])] = {
    leftAndRight
      .groupBy(_._1)
      .mapValues {
        _.map(_._2).toList.sortBy(identity)
          .scanLeft((Option.empty[(Long, (U, Option[V]))], Option.empty[(Long, (Option[V], V))])) {
            case ((_, None), (time, Left(u))) =>
              /*
                 * This is a lookup, but there is no value for this key
                 */
              val joinResult = Some((time, (u, None)))
              val sumResult = Semigroup.sumOption(valuesFn(time, (u, None))).map(v => (time, (None, v._2)))
              (joinResult, sumResult)
            case ((_, Some((_, (optv, v)))), (time, Left(u))) =>
              /*
                 * This is a lookup, and there is an existing value
                 */
              val currentV = Some(sum(optv, v)) // isn't u already a sum and optu prev value?
              val joinResult = Some((time, (u, currentV)))
              val sumResult = Semigroup.sumOption(valuesFn(time, (u, currentV))).map(v => (time, (currentV, v._2)))
              (joinResult, sumResult)
            case ((_, None), (time, Right(v))) =>
              /*
                 * This is merging in new data into the store not coming in from the service
                 * (either from the store history or from a merge after the leftJoin, but
                 * There was previously no data.
                 */
              val joinResult = None
              val sumResult = Some((time, (None, v)))
              (joinResult, sumResult)
            case ((_, Some((_, (optv, oldv)))), (time, Right(v))) =>
              /*
                 * This is the case where we are updating a non-empty key. This should
                 * only be triggered by a merged data-stream after the join since
                 * store initialization
                 */
              val joinResult = None
              val currentV = Some(sum(optv, oldv))
              val sumResult = Some((time, (currentV, v)))
              (joinResult, sumResult)
          }
      }.toList
  }

  // Test graphs

  def diamondJobInScala[T, K, V: Monoid](source: TraversableOnce[T])(fnA: T => TraversableOnce[(K, V)])(fnB: T => TraversableOnce[(K, V)]): Map[K, V] = {
    val stream = source.toStream
    val left = stream.flatMap(fnA)
    val right = stream.flatMap(fnB)
    MapAlgebra.sumByKey(left ++ right)
  }

  def diamondJob[P <: Platform[P], T, K, V: Monoid](source: Producer[P, T], sink: P#Sink[T], store: P#Store[K, V])(fnA: T => TraversableOnce[(K, V)])(fnB: T => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] = {
    val written = source.write(sink)
    val left = written.flatMap(fnA)
    val right = written.flatMap(fnB)
    left.merge(right).sumByKey(store)
  }

  def singleStepInScala[T, K, V: Monoid](source: TraversableOnce[T])(fn: T => TraversableOnce[(K, V)]): Map[K, V] =
    MapAlgebra.sumByKey(
      source.flatMap(fn)
    )

  def singleStepJob[P <: Platform[P], T, K, V: Monoid](source: Producer[P, T], store: P#Store[K, V])(fn: T => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] =
    source
      .flatMap(fn).name("FM")
      .sumByKey(store)

  def twinStepOptionMapFlatMapScala[T1, T2, K, V: Monoid](source: TraversableOnce[T1])(fnA: T1 => Option[T2], fnB: T2 => TraversableOnce[(K, V)]): Map[K, V] =
    MapAlgebra.sumByKey(
      source.flatMap(fnA(_).iterator).flatMap(fnB)
    )

  def twinStepOptionMapFlatMapJob[P <: Platform[P], T1, T2, K, V: Monoid](source: Producer[P, T1], store: P#Store[K, V])(fnA: T1 => Option[T2], fnB: T2 => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] =
    source
      .optionMap(fnA)
      .flatMap(fnB)
      .sumByKey(store)

  def singleStepMapKeysInScala[T, K1, K2, V: Monoid](source: TraversableOnce[T])(fnA: T => TraversableOnce[(K1, V)], fnB: K1 => TraversableOnce[K2]): Map[K2, V] =
    MapAlgebra.sumByKey(
      source.flatMap(fnA).flatMap { x => fnB(x._1).map((_, x._2)) }
    )

  def singleStepMapKeysJob[P <: Platform[P], T, K1, K2, V: Monoid](source: Producer[P, T], store: P#Store[K2, V])(fnA: T => TraversableOnce[(K1, V)], fnB: K1 => TraversableOnce[K2]): TailProducer[P, (K2, (Option[V], V))] =
    source
      .flatMap(fnA)
      .flatMapKeys(fnB)
      .sumByKey(store)

  def repeatedTupleLeftJoinInScala[T, U, JoinedU, K, V: Monoid](source: TraversableOnce[T])(service: K => Option[JoinedU])(preJoinFn: T => TraversableOnce[(K, U)])(postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): Map[K, V] =
    MapAlgebra.sumByKey(
      source
        .flatMap(preJoinFn)
        .flatMap { case (k, v) => List((k, v), (k, v)) }
        .map { case (k, v) => (k, (v, service(k))) }
        .flatMap(postJoinFn)
    )

  def repeatedTupleLeftJoinJob[P <: Platform[P], T, U, JoinedU, K, V: Monoid](
    source: Producer[P, T],
    service: P#Service[K, JoinedU],
    store: P#Store[K, V])(preJoinFn: T => TraversableOnce[(K, U)])(postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] =
    source
      .name("My named source")
      .flatMap(preJoinFn)
      .flatMap { case (k, v) => List((k, v), (k, v)) }
      .leftJoin(service)
      .name("My named flatmap")
      .flatMap(postJoinFn)
      .sumByKey(store)

  def leftJoinInScala[T, U, JoinedU, K, V: Monoid](source: TraversableOnce[T])(service: K => Option[JoinedU])(preJoinFn: T => TraversableOnce[(K, U)])(postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): Map[K, V] =
    MapAlgebra.sumByKey(
      source
        .flatMap(preJoinFn)
        .map { case (k, v) => (k, (v, service(k))) }
        .flatMap(postJoinFn)
    )

  def leftJoinJob[P <: Platform[P], T, U, JoinedU, K, V: Monoid](
    source: Producer[P, T],
    service: P#Service[K, JoinedU],
    store: P#Store[K, V])(preJoinFn: T => TraversableOnce[(K, U)])(postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] =
    source
      .name("My named source")
      .flatMap(preJoinFn)
      .leftJoin(service)
      .name("My named flatmap")
      .flatMap(postJoinFn)
      .sumByKey(store)

  def leftJoinWithFlatMapValuesInScala[T, U, JoinedU, K, V: Monoid](source: TraversableOnce[T])(service: K => Option[JoinedU])(preJoinFn: T => TraversableOnce[(K, U)])(postJoinFn: ((U, Option[JoinedU])) => TraversableOnce[V]): Map[K, V] =
    MapAlgebra.sumByKey(
      source
        .flatMap(preJoinFn)
        .map { case (k, v) => (k, (v, service(k))) }
        .flatMap { case (k, v) => postJoinFn(v).map { v => (k, v) } }
    )

  def leftJoinJobWithFlatMapValues[P <: Platform[P], T, U, JoinedU, K, V: Monoid](
    source: Producer[P, T],
    service: P#Service[K, JoinedU],
    store: P#Store[K, V])(preJoinFn: T => TraversableOnce[(K, U)])(postJoinFn: ((U, Option[JoinedU])) => TraversableOnce[V]): TailProducer[P, (K, (Option[V], V))] =
    source
      .name("My named source")
      .flatMap(preJoinFn)
      .leftJoin(service)
      .name("My named flatmap")
      .flatMapValues(postJoinFn)
      .sumByKey(store)

  def leftJoinWithStoreInScala[T1, T2, U, JoinedU: Monoid, K: Ordering, V: Monoid](source1: TraversableOnce[T1], source2: TraversableOnce[T2])(simpleFM1: T1 => TraversableOnce[(Long, (K, JoinedU))])(simpleFM2: T2 => TraversableOnce[(Long, (K, U))])(postJoinFn: ((Long, (K, (U, Option[JoinedU])))) => TraversableOnce[(Long, (K, V))]): (Map[K, JoinedU], Map[K, V]) = {

    val firstStore = MapAlgebra.sumByKey(
      source1
        .flatMap(simpleFM1)
        .map { case (_, kju) => kju } // drop the time from the key for the store
    )

    // create the delta stream
    val sumStream: Iterable[(Long, (K, (Option[JoinedU], JoinedU)))] =
      source1
        .flatMap(simpleFM1)
        .toList.groupBy(_._1)
        .mapValues {
          _.map { case (time, (k, joinedu)) => (k, joinedu) }
            .groupBy(_._1)
            .mapValues { l => scanSum(l.iterator.map(_._2)).toList }
            .toIterable
            .flatMap { case (k, lv) => lv.map { case (optju, ju) => (k, (optju, ju)) } }
        }
        .toIterable
        .flatMap { case (time, lv) => lv.map { case (k, (optju, ju)) => (time, (k, (optju, ju))) } }

    // zip the left and right streams
    val leftAndRight: Iterable[(K, (Long, Either[U, JoinedU]))] =
      source2
        .flatMap(simpleFM2)
        .toList
        .map { case (time, (k, u)) => (k, (time, Left(u))) }
        .++(sumStream.map { case (time, (k, (optju, ju))) => (k, (time, Right(ju))) })

    // scan left to join the left values and the right summing result stream
    val resultStream: List[(Long, (K, (U, Option[JoinedU])))] =
      leftAndRight
        .groupBy(_._1)
        .mapValues {
          _.map(_._2).toList.sortBy(identity)
            .scanLeft(Option.empty[(Long, JoinedU)], Option.empty[(Long, U, Option[JoinedU])]) {
              case ((None, result), (time, Left(u))) => {
                // The was no value previously
                (None, Some((time, u, None)))
              }
              case ((prev @ Some((oldt, ju)), result), (time, Left(u))) => {
                // gate the time for window join?
                (prev, Some((time, u, Some(ju))))
              }
              case ((None, result), (time, Right(joined))) => {
                (Some((time, joined)), None)
              }

              case ((Some((oldt, oldJ)), result), (time, Right(joined))) => {
                val nextJoined = Semigroup.plus(oldJ, joined)
                (Some((time, nextJoined)), None)
              }
            }
        }.toList.flatMap { case (k, lv) => lv.map { case ((_, optuju)) => (k, optuju) } }
        .flatMap {
          case (k, opt) => opt.map { case (time, u, optju) => (time, (k, (u, optju))) }
        }

    // compute the final store result after join
    val finalStore = MapAlgebra.sumByKey(
      resultStream
        .flatMap(postJoinFn)
        .map { case (time, (k, v)) => (k, v) } // drop the time
    )
    (firstStore, finalStore)
  }

  def leftJoinWithStoreJob[P <: Platform[P], T1, T2, U, K, JoinedU: Monoid, V: Monoid](
    source1: Producer[P, T1],
    source2: Producer[P, T2],
    storeAndService: P#Store[K, JoinedU] with P#Service[K, JoinedU],
    store: P#Store[K, V])(simpleFM1: T1 => TraversableOnce[(K, JoinedU)])(simpleFM2: T2 => TraversableOnce[(K, U)])(postJoinFn: ((K, (U, Option[JoinedU]))) => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] = {

    // sum to first store
    val dag1: Summer[P, K, JoinedU] = source1
      .flatMap(simpleFM1)
      .sumByKey(storeAndService)

    // join second source with stream from first store
    val dag2: Summer[P, K, V] = source2
      .flatMap(simpleFM2)
      .leftJoin(storeAndService)
      .flatMap(postJoinFn)
      .sumByKey(store)

    dag1.also(dag2)
  }

  def leftJoinWithDependentStoreInScala[T, U, K: Ordering, V: Monoid](source: TraversableOnce[T])(simpleFM: T => TraversableOnce[(Long, (K, U))])(flatMapValuesFn: ((Long, (U, Option[V]))) => TraversableOnce[(Long, V)]): Map[K, V] = {

    // zip the left and right streams
    val leftAndRight: Iterable[(K, (Long, Either[U, V]))] =
      source
        .flatMap(simpleFM)
        .toList
        .map { case (time, (k, u)) => (k, (time, Left(u))) }

    // scan left to join the left values and the right summing result stream
    val resultStream = loopJoinInScala(leftAndRight, flatMapValuesFn)

    // compute the final store result after join
    val rightStream = resultStream
      .flatMap { case (k, lopts) => lopts.map { case ((_, optoptv)) => (k, optoptv) } }

    // compute the final store result after join
    MapAlgebra.sumByKey(
      rightStream
        .flatMap { case (k, opt) => opt.map { case (time, (optv, v)) => (k, v) } } // drop time and opt[v]
    )
  }

  def leftJoinWithDependentStoreJob[P <: Platform[P], T, V1, U, K, V: Monoid](
    source1: Producer[P, T],
    storeAndService: P#Store[K, V] with P#Service[K, V])(simpleFM1: T => TraversableOnce[(K, U)])(valuesFlatMap1: ((U, Option[V])) => TraversableOnce[V1])(valuesFlatMap2: (V1) => TraversableOnce[V]): TailProducer[P, (K, (Option[V], V))] = {

    source1
      .flatMap(simpleFM1)
      .leftJoin(storeAndService)
      .flatMapValues(valuesFlatMap1)
      .flatMapValues(valuesFlatMap2)
      .sumByKey(storeAndService)
  }

  def leftJoinWithDependentStoreJoinFanoutInScala[T, U, K: Ordering, V: Monoid, V1: Monoid](source: TraversableOnce[T])(simpleFM: T => TraversableOnce[(Long, (K, U))])(flatMapValuesFn: ((Long, (U, Option[V]))) => TraversableOnce[(Long, V)])(flatMapFn: ((Long, (K, (U, Option[V])))) => TraversableOnce[(Long, (K, V1))]): (Map[K, V], Map[K, V1]) = {

    // zip the left and right streams
    val leftAndRight: Iterable[(K, (Long, Either[U, V]))] =
      source
        .flatMap(simpleFM)
        .toList
        .map { case (time, (k, u)) => (k, (time, Left(u))) }

    // scan left to join the left values and the right summing result stream
    val resultStream = loopJoinInScala(leftAndRight, flatMapValuesFn)

    val leftStream = resultStream
      .flatMap { case (k, lopts) => lopts.map { case ((optuoptv, _)) => (k, optuoptv) } }

    val rightStream = resultStream
      .flatMap { case (k, lopts) => lopts.map { case ((_, optoptv)) => (k, optoptv) } }

    // compute the first store using the join stream as input
    val storeAfterFlatMap = MapAlgebra.sumByKey(
      leftStream
        .flatMap { case (k, opt) => opt.map { case (time, (u, optv)) => (time, (k, (u, optv))) } }
        .flatMap(flatMapFn(_))
        .map { case (time, (k, v)) => (k, v) } // drop the time
    )

    // compute the final store result after join
    val storeAfterJoin = MapAlgebra.sumByKey(
      rightStream
        .flatMap { case (k, opt) => opt.map { case (time, (optv, v)) => (k, v) } } // drop time and opt[v]
    )

    (storeAfterJoin, storeAfterFlatMap)
  }

  def leftJoinWithDependentStoreJoinFanoutJob[P <: Platform[P], T1, V1: Monoid, U, K, V: Monoid](
    source1: Producer[P, T1],
    storeAndService: P#Store[K, V] with P#Service[K, V],
    store: P#Store[K, V1])(simpleFM1: T1 => TraversableOnce[(K, U)])(valuesFlatMap1: ((U, Option[V])) => TraversableOnce[V])(flatMapFn: ((K, (U, Option[V]))) => TraversableOnce[(K, V1)]): TailProducer[P, (K, (Option[V], V))] = {

    val join: KeyedProducer[P, K, (U, Option[V])] = source1
      .flatMap(simpleFM1)
      .leftJoin(storeAndService)

    val dependentSum: Summer[P, K, V] = join
      .flatMapValues(valuesFlatMap1)
      .sumByKey(storeAndService)

    val indepSum: Summer[P, K, V1] = join
      .flatMap(flatMapFn)
      .sumByKey(store)

    indepSum.also(dependentSum)
  }

  def realJoinTestJob[P <: Platform[P], T1, T2, T3, T4, K1, K2, U, JoinedU, V: Monoid](
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
    postJoin: ((K1, (U, Option[JoinedU]))) => TraversableOnce[(K2, V)]): TailProducer[P, (K2, (Option[V], V))] = {
    val data1 = source1.flatMap(simpleFM1)
    val data2 = source2.flatMap(simpleFM2)
    val data3 = source3.flatMap(simpleFM3)
    val data4 = source4.map(preJoin).leftJoin(service).flatMap(postJoin)
    data1.merge(data2).merge(data3).merge(data4).sumByKey(store).name("Customer Supplied Job")
  }

  def writtenPostSum[P <: Platform[P], T, K, V: Monoid](source: Producer[P, T], sink: P#Sink[(K, (Option[V], V))], store: P#Store[K, V])(fnA: T => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] = {
    val left = source.flatMap(fnA)
    left.sumByKey(store).write(sink)
  }

  def realJoinTestJobInScala[P <: Platform[P], T1, T2, T3, T4, K1, K2, U, JoinedU, V: Monoid](
    source1: List[T1],
    source2: List[T2],
    source3: List[T3],
    source4: List[T4],
    service: K1 => Option[JoinedU],
    simpleFM1: T1 => TraversableOnce[(K2, V)],
    simpleFM2: T2 => TraversableOnce[(K2, V)],
    simpleFM3: T3 => TraversableOnce[(K2, V)],
    preJoin: T4 => (K1, U),
    postJoin: ((K1, (U, Option[JoinedU]))) => TraversableOnce[(K2, V)]): Map[K2, V] = {
    val data1 = source1.flatMap(simpleFM1)
    val data2 = source2.flatMap(simpleFM2)
    val data3 = source3.flatMap(simpleFM3)
    val data4 = source4.map(preJoin).map { case (k, v) => (k, (v, service(k))) }
      .flatMap(postJoin)
    MapAlgebra.sumByKey(data1 ::: data2 ::: data3 ::: data4)
  }

  def multipleSummerJobInScala[T1, T2, K1, V1: Monoid, K2, V2: Monoid](source: List[T1])(fnR: T1 => TraversableOnce[T2], fnA: T2 => TraversableOnce[(K1, V1)], fnB: T2 => TraversableOnce[(K2, V2)]): (Map[K1, V1], Map[K2, V2]) = {
    val mapA = MapAlgebra.sumByKey(source.flatMap(fnR).flatMap(fnA))
    val mapB = MapAlgebra.sumByKey(source.flatMap(fnR).flatMap(fnB))
    (mapA, mapB)
  }

  def multipleSummerJob[P <: Platform[P], T1, T2, K1, V1: Monoid, K2, V2: Monoid](source: Producer[P, T1], store1: P#Store[K1, V1], store2: P#Store[K2, V2])(fnR: T1 => TraversableOnce[T2], fnA: T2 => TraversableOnce[(K1, V1)],
    fnB: T2 => TraversableOnce[(K2, V2)]): TailProducer[P, (K2, (Option[V2], V2))] = {
    val combined = source.flatMap(fnR)
    val calculated = combined.flatMap(fnB).sumByKey(store2)
    combined.flatMap(fnA).sumByKey(store1).also(calculated)
  }

  def mapOnlyJob[P <: Platform[P], T, U](
    source: Producer[P, T],
    sink: P#Sink[U])(mapOp: T => TraversableOnce[U]): TailProducer[P, U] =
    source
      .flatMap(mapOp)
      .write(sink)

  def lookupJob[P <: Platform[P], T, U](
    source: Producer[P, T], srv: P#Service[T, U], sink: P#Sink[(T, U)]): TailProducer[P, (T, U)] =
    source.lookup(srv).collectValues { case Some(v) => v }.write(sink)

  def lookupJobInScala[T, U](in: List[T], srv: (T) => Option[U]): List[(T, U)] =
    in.map { t => (t, srv(t)) }.collect { case (t, Some(u)) => (t, u) }

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

  def jobWithStats[P <: Platform[P], T, K, V: Monoid](id: JobId, source: Producer[P, T], store: P#Store[K, V])(fn: T => TraversableOnce[(K, V)]): TailProducer[P, (K, (Option[V], V))] = {
    implicit val jobID: JobId = id
    val origCounter = Counter(Group("counter.test"), Name("orig_counter"))
    val fmCounter = Counter(Group("counter.test"), Name("fm_counter"))
    val fltrCounter = Counter(Group("counter.test"), Name("fltr_counter"))
    source
      .flatMap { x => origCounter.incr; fn(x) }.name("FM")
      .filter { x => fmCounter.incrBy(2); true }
      .map { x => fltrCounter.incr; x }
      .sumByKey(store)
  }
}

class TestGraphs[P <: Platform[P], T: Manifest: Arbitrary, K: Arbitrary, V: Arbitrary: Equiv: Monoid](platform: P)(
    store: () => P#Store[K, V])(sink: () => P#Sink[T])(
        sourceMaker: TraversableOnce[T] => Producer[P, T])(
            toLookupFn: P#Store[K, V] => (K => Option[V]))(
                toSinkChecker: (P#Sink[T], List[T]) => Boolean)(
                    run: (P, P#Plan[_]) => Unit) {

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
    TestGraphs.diamondJobInScala(items)(fnA)(fnB).forall {
      case (k, v) =>
        val lv = lookupFn(k).getOrElse(Monoid.zero)
        val eqv = Equiv[V].equiv(v, lv)
        if (!eqv) {
          println(s"in diamondChecker: $k, $v is scala result, but platform gave $lv")
        }
        eqv
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
    TestGraphs.singleStepInScala(items)(fn).forall {
      case (k, v) =>
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
    ).forall {
        case (k, v) =>
          val lv = lookupFn(k).getOrElse(Monoid.zero)
          Equiv[V].equiv(v, lv)
      }
  }
}
