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

package com.twitter.summingbird.service

import cascading.flow.FlowDef
import com.twitter.algebird.Monoid
import com.twitter.scalding.{ TypedPipe, TDsl, Mode }
import com.twitter.summingbird.AbstractJob
import com.twitter.summingbird.builder.CompletedBuilder
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.scalding.store.{BatchStore, BatchReadableStore, IntermediateStore}
import com.twitter.summingbird.batch.{ BatchID, Batcher }

import java.io.Serializable

object BatchService extends Serializable {
  def nonZeroPlus[T: Monoid](a: Option[T], b: Option[T]): Option[T] =
    Monoid.plus(a, b).flatMap(Monoid.nonZeroOption(_))

  /**
      * Implicit ordering on an either that doesn't care about the
      * actual container values.
      */
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

  def rollingDeltas[K, V](
    aggregated: TypedPipe[(K, V)],
    deltaStore: IntermediateStore[K, V],
    missingBatches: Iterable[BatchID]
  )(implicit env: ScaldingEnv, fd: FlowDef): TypedPipe[(Long, K, V)] = {
    val aggWithTime = aggregated.map { case (k, v) => (Long.MinValue, k, v) }
    val deltaPipes = deltaStore.read(env, missingBatches)
    aggWithTime ++ deltaPipes
  }

  /**
    * timeJoin simulates the behavior of a realtime system attempting
    * to leftJoin (K, V) pairs against some other value type (JoinedV)
    * by performing realtime lookups on a key-value MergeableStore
    * (https://github.com/twitter/storehaus/blob/develop/storehaus-algebra/src/main/scala/com/twitter/storehaus/algebra/MergeableStore.scala).
    *
    * An example would join (K, V) pairs of (URL, Username) against a
    * service of (URL, ImpressionCount). The result of this join would
    * be a pipe of (ShortenedURL, (Username,
    * Option[ImpressionCount])).
    *
    * To simulate this behavior, timeJoin accepts pipes of key-value
    * pairs with an explicit time value T attached. T must have some
    * sensical ordering. The semantics are, if one were to hit the
    * right pipe's simulated realtime service at any time between
    * T(tuple) T(tuple + 1), one would receive Some((K,
    * JoinedV)(tuple)).
    *
    * The entries in the left pipe's tuples have the following
    * meaning:
    *
    * T: The the time at which the (K, V) lookup occurred.
    * K: the join key.
    * V: the current value for the join key.
    *
    * The right pipe's entries have the following meaning:
    *
    * T: The time at which the "service" was fed an update
    * K: the join K.
    * V: the delta received by the MergeableStore "service"
    *
    * Before the time T in the right pipe's very first entry, the
    * simulated "service" will return None. After this time T, the
    * right side will return None only if the deltas add up to
    * Monoid.zero; else, the service will return Some(joinedV).
    *
    */
  def timeJoin[T, K, V, JoinedV](left: TypedPipe[(T, K, V)], right: TypedPipe[(T, K, JoinedV)], reducers: Int)
    (implicit kOrd: Ordering[K], tOrd: Ordering[T], monoid: Monoid[JoinedV])
      : TypedPipe[(T, K, (V, Option[JoinedV]))] = {
    val joined: TypedPipe[(K, (Option[JoinedV], Option[(T, V, Option[JoinedV])]))] =
      left.map { case (t, k, v) => (t, k, Left(v): Either[V, JoinedV]) }
        .++(right.map { case (t, k, joinedV) => (t, k, Right(joinedV): Either[V, JoinedV]) })
        .map { case (t, k, either) => (k, (t, either)) }
        .group
        .withReducers(reducers)
        .sortBy[T] { _._1 }
    /**
      * Grouping by K leaves values of (T, Either[V, JoinedV]). Sort
      * by time and scanLeft. The iterator will now represent pairs of
      * T and either new values to join against or updates to the
      * simulated "realtime store" described above.
      */
        .scanLeft(
          /**
            * In the simulated realtime store described above, this
            * None is the value in the store at the current
            * time. Because we sort by time and scan forward, this
            * value will be updated with a new value every time a
            * Right(delta) shows up in the iterator.
            *
            * The second entry in the pair will be None when the
            * JoinedV is updated and Some(newValue) when a (K, V)
            * shows up and a new join occurs.
            */
          (None: Option[JoinedV], None: Option[(T, V, Option[JoinedV])])
        ) { case ((lastJoined, _), (thisTime: T, newPair: Either[V, JoinedV])) =>
            newPair match {
              // Left(v) means that we have a new value from the left
              // pipe that we need to join against the current
              // "lastJoined" value sitting in scanLeft's state. This
              // is equivalent to a lookup on the data in the right
              // pipe at time "thisTime".
              case Left(v) => (lastJoined, Some((thisTime, v, lastJoined)))

              // Right(joinedV) means that we've received a new delta
              // to merge into the simulated realtime service
              // described in the comments above. Add the delta to the
              // existing value sitting in the simulated service (as
              // joinedV) and return it to become the scanLeft's next
              // state.
              case Right(delta) => (nonZeroPlus(lastJoined, Some(delta)), None)
            }
        }.toTypedPipe

    for {
      // Now, get rid of residual state from the scanLeft above:
      (k, (_, optV)) <- joined

      // filter out every event that produced a Right(delta) above,
      // leaving only the leftJoin events that occurred above:
      (t, v, optJoined) <- optV
    } yield (t, k, (v, optJoined))
  }

  // TODO: this seems to be generally useful. put it somewhere else during the scalding/storm/builder code splitting.
  def getFirstBatchID(offlineStore: BatchStore[_, _])
      (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv, batcher: Batcher) =
    env.startBatch(batcher)
      .orElse {
        for {
          (batchID, _) <- offlineStore.readLatest(env)
        } yield batchID
      }
}

/**
  * Offline service to be used when joining against other Summingbird
  * jobs. To make this cleaner, we should implement a config trait
  * which has these various items defined and create a constructor in
  * the companion object that extracts those items from the dataset.
  */
class BatchService[K, JoinedV](
  otherStore: BatchReadableStore[K, (BatchID, JoinedV)],
  deltaStore: IntermediateStore[K, JoinedV],
  otherBatcher: Batcher)
  (implicit kOrd: Ordering[K], batcher: Batcher, monoid: Monoid[JoinedV])
    extends OfflineService[K, JoinedV] {
  import BatchService._

  override def leftJoin[V](pipe: TypedPipe[(Long, K, V)])
    (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv) = {
    import TDsl._

    val thisBuilder = env.builder

    (for {
      lastProcessed <- getFirstBatchID(thisBuilder.store.offlineStore)
      extremities = (lastProcessed, lastProcessed + env.batches - 1)
      requiredDeltas = batcher.enclosedBy(extremities, otherBatcher)
      otherAggregatedUpTo: BatchID <- otherStore.availableBatches(env).find(_ <= requiredDeltas.min)
      aggregated <- otherStore.read(otherAggregatedUpTo, env)
      remainingBatches = BatchID.range(otherAggregatedUpTo, requiredDeltas.last).toList
      aggWithoutBatch = aggregated.map { case (k, (_, v)) => (k, v) }
      deltaPipe = rollingDeltas[K, JoinedV](aggWithoutBatch, deltaStore, remainingBatches)
    } yield timeJoin(pipe, deltaPipe, env.reducers))
      .getOrElse(sys.error("Not enough data available for a time-based join."))
  }
}
