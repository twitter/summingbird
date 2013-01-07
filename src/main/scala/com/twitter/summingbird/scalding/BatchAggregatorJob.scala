package com.twitter.summingbird.scalding

import com.twitter.algebird.Monoid
import com.twitter.scalding.{ Job => ScaldingJob, TDsl, IterableSource, Mode }
import com.twitter.summingbird.builder.CompletedBuilder

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
 * resulting aggregate kv-pairs into the user-supplied Sink.
 *
 * Every time this job runs, Summingbird stores metadata about what batches
 * were processed alongside the data output. Every time the BatchAggregatorJob
 * runs it uses this information along with the supplied ScaldingEnv to
 * figure out which batches of data to source for the next run. Because
 * the BatchAggregatorJob manages its own state, the user can run this job
 * in a `while(true)` loop without worrying about data corruption.
 */

class BatchAggregatorJob[T,K,V](@transient bldr: CompletedBuilder[T,K,V],
                                @transient env: ScaldingEnv)
extends ScaldingJob(env.args) {
  import TDsl._

  implicit val monoid: Monoid[V] = bldr.sink.monoid
  implicit val kord: Ordering[K] = bldr.flatMappedBuilder.keyOrdering

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

  val batcher = bldr.sink.batcher
  implicit val tord: Ordering[T] = new Ordering[T] with java.io.Serializable {
    override def compare(l: T, r: T) = batcher.timeComparator.compare(l,r)
  }

  val (oldBatchIdUpperBound, latestAggregated) = env.startBatch(batcher)
    .map { (_, mappableToTypedPipe(IterableSource(Seq[(K,V)]()))) }
    .getOrElse(bldr.sink.readLatest(env))

  // This is before all the stuff we add in, so add a None time:
  val latestWithTime = latestAggregated.map { kv => (None : Option[T], kv._1, kv._2) }

  val summed = (bldr.flatMappedBuilder.getFlatMappedPipe(batcher, oldBatchIdUpperBound, env)
    .map { tkv => (Some(tkv._1), tkv._2, tkv._3) } //lift T to Option[T] to get correct sorting
      ++ latestWithTime)
    .groupBy { _._2 } // key
    .withReducers(env.reducers)
    .sortBy { _._1 } // time (None, which is in latestWithTime, comes first)
    .mapValues { tkv => tkv._3 } // keep just the V
    .sum // over the V.
    .toTypedPipe // Now we just have (K,V)
    .filter { kv => monoid.isNonZero(kv._2) }

  // Write it out:
  bldr.sink.write(env, summed)

  override def next = {
    // Write the batch upper bound, so we can recover the oldBatchIdUpperBound next time
    bldr.sink.commit(oldBatchIdUpperBound, env)
    None
  }
}
