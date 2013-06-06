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

package com.twitter.summingbird.scalding

import com.twitter.algebird.Monoid
import com.twitter.bijection.{ Bijection, ImplicitBijection }
import com.twitter.bijection.algebird.BijectedMonoid
import com.twitter.scalding.{ Job => ScaldingJob, _ }
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.builder.FlatMappedBuilder
import com.twitter.summingbird.scalding.store._

import java.util.Date

/**
 * Scalding job representing the batch computation of the
 * Summingbird workflow.
 *
 * BatchAggregatorJob pulls Event instances out of some EventSource,
 * transforms these into Key-Value pairs with a user-supplied FlatMapper,
 * merges all values for each key with a Monoid[Value] and sinks the
 * resulting aggregate kv-pairs into the user-supplied Store.
 *
 * Every time this job runs, Summingbird stores metadata about what batches
 * were processed alongside the data output. Every time the BatchAggregatorJob
 * runs it uses this information along with the supplied ScaldingEnv to
 * figure out which batches of data to source for the next run. Because
 * the BatchAggregatorJob manages its own state, the user can run this job
 * in a `while(true)` loop without worrying about data corruption.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class BatchAggregatorJob[K: Ordering, V: Monoid](
  @transient bldr: FlatMappedBuilder[K,V],
  @transient env: ScaldingEnv,
  batcher: Batcher,
  @transient offlineStore: BatchStore[K, (BatchID, V)],
  monoidIsCommutative: Boolean,
  @transient intermediateStore: Option[IntermediateStore[K, V]])
extends ScaldingJob(env.args) {
  import TDsl._
  import Dsl._

  /**
    * Set the job name in the Hadoop JobTracker to the Summingbird job
    * name. (Without this override every job shows up as
    * "BatchAggregatorJob".)
    */
  override def name = env.jobName

  /**
    * Replace Scalding's default implementation of
    *  cascading.kryo.KryoSerialization with Summingbird's custom
    * extension. SummingbirdKryoHadoop performs every registration in
    * KryoHadoop, then registers event, time, key and value codecs
    * using chill's BijectiveSerializer.
    */
  override def config(implicit mode: Mode) =
    super.config(mode) ++ Map(
      "io.serializations" ->
        (ioSerializations ++ List(classOf[SummingbirdKryoHadoop].getName)).mkString(",")
    )

  /**
    * Performs a "sum" on the grouped pipe and filters out all
    * zeros. If the supplied Monoid is an instance of
    * BijectedMonoid[U, V], the method will extract the underlying
    * Monoid[U], convert all values over to U, run the monoid and
    * convert back before returning the TypedPipe. This should help
    * with latency in almost all cases that use a BijectedMonoid.
    */
  def sum[U](pipe: Grouped[K, V]): TypedPipe[(K, V)] =
    unpackBijectedMonoid[U].map { case (bijection, monoid) =>
      implicit val m: Monoid[U] = monoid
      pipe.mapValues(bijection.invert(_))
        .sum
        .filter { case (_, u) => Monoid.isNonZero(u) }
        .map { case (k, u) => k -> bijection(u) }
    }.getOrElse {
      pipe.sum // over the V.
        .filter { case (k, v) => Monoid.isNonZero(v) }
    }

  /**
    * If the supplied implicit Monoid[V] is an instance of
    * BijectedMonoid[U, V], Returns the backing Bijection[U, V] and
    * Monoid[U]; else None.
    *
    * TODO: Make the referenced fields on
    * com.twitter.bijection.algebird.Bijected{Monoid,Semigroup,Ring,Group}
    * into vals and remove the reflection here.
    */
  def unpackBijectedMonoid[U]: Option[(Bijection[U, V], Monoid[U])] = {
    implicitly[Monoid[V]] match {
      case m: BijectedMonoid[_, _] => {
        def getField[F: ClassManifest](fieldName: String): F = {
          val f = classOf[BijectedMonoid[_, _]].getDeclaredField(fieldName)
          f.setAccessible(true)
          implicitly[ClassManifest[F]]
            .erasure.asInstanceOf[Class[F]].cast(f.get(m))
        }
        Some((
          getField[ImplicitBijection[U, V]]("bij").bijection,
          getField[Monoid[U]]("monoid")
        ))
      }
      case _ => None
    }
  }

  /**
    * latestAggregated is a TypedPipe representing existing
    * Summingbird dataset of key-value pairs. oldBatchIdUpperBound is
    * the BatchID just after the maximum BatchID of the key-value
    * pairs inside of latestAggregated. That is to say, the current
    * delta will process events from BatchID equal to
    * oldBatchIdUpperBound.
    */
  val (oldBatchIdUpperBound, latestAggregated): (BatchID, TypedPipe[(K, V)]) =
    env.startBatch(batcher)
      .flatMap { id => BatchReadableStore.empty[K, V](id).readLatest(env) }
      .orElse {
      for {
        (batchID, pipe) <- offlineStore.readLatest(env)
      } yield (batchID, pipe map { case (k, (b, v)) => (k, v) })
    }.getOrElse(sys.error("Not the initial batch, and no previous batch"))

  /**
    * New key-value pairs from the current batch's run.
    */
  val delta: TypedPipe[(Long, K, V)] =
    bldr.getFlatMappedPipe(batcher, oldBatchIdUpperBound, env)

  /**
    * Now begins the actual scalding job.
    *
    * The job merges in new data as an incremental update to some
    * existing dataset. Because the existing data is pre-aggregated,
    * we don't have times for any of the key-value pairs. If the
    * Monoid[V] is not commutative, this is cause for concern; how do
    * we ensure that old data always appears before new data in the
    * sort?
    *
    * We solve this by sorting on Option[Long] instead of Long,
    * tagging all old data with None and lifting every timestamp in
    * new key-value pairs up to Some(timestamp). Because None <
    * Some(x), the data will be sorted correctly.
    */
  val mergedPairs =
    if (monoidIsCommutative) {
      (latestAggregated ++ delta.map { case (_, k, v) => (k, v) })
        .group
        .withReducers(env.reducers)
    } else {
      (latestAggregated.map { case (k, v) => (None : Option[Long], k, v) }
        ++ delta.map[(Option[Long],K,V)] { case (t, k, v) => (Some(t), k, v) })
        .groupBy { case (_, k, _) => k }
        .withReducers(env.reducers)
        .sortBy { case (optT, _, _) => optT }
        .mapValues { case (_, _, v) => v }
    }

  val newBatchID = oldBatchIdUpperBound + env.batches
  val finalPipe = sum(mergedPairs)
    .map { case (k, v) => (k, (newBatchID, v)) }

  // Write the new aggregated dataset out to the offlineStore:
  offlineStore.write(env, finalPipe)

  /**
   * If the job has been configured to do so, then store intermediate
   * data so other jobs can join against it in the future. This stores
   * the tuple (Long, K, V) for each batch in a file. With this value
   * one can compute the value of K at some time.
   */
  intermediateStore.foreach {store =>
    (0 until env.batches).foreach { i =>
      val currentBatch = oldBatchIdUpperBound + i
      val a = delta.filter {case (l, k, v) => batcher.batchOf(new Date(l)) == currentBatch }
      store.dump(env, a, oldBatchIdUpperBound + i)
    }
  }

  /**
    * Commit the new upper bound into the offlineStore so we can recover
    * the oldBatchIdUpperBound on the next run.
    */
  override def next = {
    offlineStore.commit(newBatchID, env)
    None
  }
}
