/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.scalding

import com.twitter.algebird.Monoid
import com.twitter.scalding.{ Job => ScaldingJob, TDsl, TypedPipe, IterableSource, Mode }
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.builder.CompletedBuilder
import com.twitter.summingbird.scalding.store.BatchReadableStore

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
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
 */

class BatchAggregatorJob[T,K: Ordering,V: Monoid](@transient bldr: CompletedBuilder[_,T,K,V],
                                                  @transient env: ScaldingEnv,
                                                  monoidIsCommutative: Boolean)
extends ScaldingJob(env.args) {
  import TDsl._

  override def config(implicit mode: Mode) = {
    // Replace Scalding's default implementation of
    // cascading.kryo.KryoSerialization with Summingbird's custom
    // extension. SummingbirdKryoHadoop performs every registration in
    // KryoHadoop, then registers event, time, key and value codecs
    // using chill's BijectiveSerializer.
    val entries =
      (ioSerializations ++
       List("com.twitter.summingbird.scalding.SummingbirdKryoHadoop")).mkString(",")
    super.config(mode) ++ Map("io.serializations" -> entries)
  }

  val batcher = bldr.batcher
  val offlineStore = bldr.store.offlineStore
  implicit val tord: Ordering[T] = new Ordering[T] with java.io.Serializable {
    override def compare(l: T, r: T) = batcher.timeComparator.compare(l,r)
  }

  val (oldBatchIdUpperBound, latestAggregated): (BatchID, TypedPipe[(K,V)]) =
    env.startBatch(batcher)
      .map { id => BatchReadableStore.empty[K,V](id).readLatest(env) }
      .getOrElse {
        val (batchID, pipe) = offlineStore.readLatest(env)
        (batchID, pipe map { case (k,(b,v)) => (k, v) })
      }

  // This is before all the stuff we add in, so add a None time:
  val latestWithTime: TypedPipe[(Option[T],K,V)] =
    latestAggregated.map { case (k, v) => (None : Option[T], k, v) }

  val grouped = (bldr.flatMappedBuilder.getFlatMappedPipe(batcher, oldBatchIdUpperBound, env)
    .map[(Option[T],K,V)] { case (t, k, v) => (Some(t), k, v) } //lift T to Option[T] to get correct sorting
      ++ latestWithTime)
    .groupBy { _._2 } // key
    .withReducers(env.reducers)

  val maybeSorted =
    if (monoidIsCommutative)
      grouped
    else
      grouped.sortBy { _._1 } // time (None, which is in latestWithTime, comes first)

  val summed = maybeSorted
    .mapValues { tkv => tkv._3 } // keep just the V
    .sum // over the V.
    .filter { case (k, v) => Monoid.isNonZero(v) }

  val newBatchID = oldBatchIdUpperBound + env.batches
  val finalPipe = summed map { case (k, v) => (k, (newBatchID, v)) }

  // Write it out:
  offlineStore.write(env, finalPipe)

  override def next = {
    // Write the batch upper bound, so we can recover the oldBatchIdUpperBound next time
    offlineStore.commit(newBatchID, env)
    None
  }
}
