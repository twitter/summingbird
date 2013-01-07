package com.twitter.summingbird.sink

import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.util.FutureUtil
import com.twitter.algebird.Monoid
import com.twitter.bijection.{ Bijection, Pivot }
import com.twitter.util.Future
import com.twitter.scalding.TypedPipe
import com.twitter.scalding.Mode
import cascading.flow.FlowDef

import FutureUtil._

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// Summingbird’s stores its aggregated key-value pairs behind the
// Sink[Time,Key,Value] interface. Like the EventSource, a Sink can be
// an OfflineSink, an OnlineSink or a combination of the two. (For a
// Summingbird job to run in some mode (online or offline), its
// sources and sinks need to implement the requisite source and sink
// interfaces. This check occurs at job submission time.)
//
// Every sink, online or offline, needs to know how Time is batched,
// how to combine values with a Monoid[Value], and how to return a
// value for a given key. Put another way, every Sink wraps a
// random-read database.

trait BaseSink[Time,Key,Value] extends java.io.Serializable {
  def batcher: Batcher[Time]
  def monoid: Monoid[Value]
  // Pivot to pivot (Key,BatchID) pairs into K -> (BatchID,V).
  // TODO: rewrite the timeOf method in the batcher in terms of the pivot.
  val pivot = Pivot[(Key,BatchID),Key,BatchID](Bijection.identity).withValue[Value]
}

// Online and Offline sinks and sources manage all coordination
// through the shared Batcher[Time] and a Monoid[Value].
//
// ## Offline Sink
//
// Offline sinks in Summingbird are typically implemented with
// read-only backing stores like Manhattan that support bulk-loads of
// an entire dataset at once. The following trait describes an
// OfflineSink[Time,Key,Value]:

trait OfflineSink[Time,Key,Value] extends BaseSink[Time,Key,Value] {
  // Return the previous upper bound batchId and the pipe covering this range
  // NONE of the items from the returned batchID should be present in the TypedPipe

  // User must implement at least one of the following two methods.
  def getOffline(k: Key): Future[Option[(BatchID,Value)]] = single(k) { multiGetOffline(_) }

  def multiGetOffline(ks: Set[Key]): Future[Map[Key,(BatchID,Value)]] =
    collectValues(ks)(getOffline _)

  def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode): (BatchID, TypedPipe[(Key,Value)])
  def write(env: ScaldingEnv, p: TypedPipe[(Key,Value)])(implicit fd: FlowDef, mode: Mode): Unit
  def commit(batchId: BatchID, env: ScaldingEnv): Unit

  def ++(other: OnlineSink[Time,Key,Value]) = CompoundSink(this, other)
}

// Summingbird uses the “write” and “commit” functions above to
// bulk-load the key-value pairs of the offline aggregation into the
// database. The Sink accepts a pipe and can write it into whatever
// Scalding Source it likes. “commit” can be used to write a
// completion file, signalling to the backing store (Manhattan) that
// the dataset is complete and ready to be served.
//
// The “readLatest” function enables incremental updates to the
// offline store. readLatest returns
//
// * the most up-to-date set of key-value pairs in the offline backing
//   store in the form of a Scalding pipe
// * The first BatchID after the most recent BatchID processed into
//   the datastore.
//
// When processing new Events, Summingbird can use this this BatchID
// to filter out all new information with a lower BatchID, providing a
// hedge against any double processing of events.

// ## Online Sink

// Online sinks are backed by random-write stores. The following trait
// specifies an OnlineSink:

trait OnlineSink[Time,Key,Value] extends BaseSink[Time,Key,Value] {
  def batchesToKeep: BatchesToKeep

  // User must implement at least one of the following two methods.
  def getOnline(k: (Key,BatchID)): Future[Option[Value]] = single(k) { multiGetOnline(_) }

  def multiGetOnline(pairs: Set[(Key,BatchID)]): Future[Map[(Key,BatchID),Value]] =
    collectValues(pairs)(getOnline _)

  def increment(pair: ((Key,BatchID),Value)): Future[Unit]

  // TODO: Remove these three values from the trait. They should be in
  // here as part of the CompletedBuilder.
  def sinkParallelism: SinkParallelism
  def decoderParallelism: DecoderParallelism
  def rpcParallelism: RpcParallelism

  // ## Helpers for multiGet.
  def ++(other: OfflineSink[Time,Key,Value]) = CompoundSink(other, this)

  protected def defaultInit(nowBatch: BatchID = batcher.currentBatch): Option[(BatchID,Value)] =
    Some((nowBatch - (batchesToKeep.count - 1), monoid.zero))

  protected def expand(init: Option[(BatchID,Value)], nowBatch: BatchID = batcher.currentBatch)
  : Iterable[(BatchID,Value)] = {
    implicit val vm = monoid
    val (initBatch,initV) = Monoid.plus(defaultInit(nowBatch), init).get
    BatchID.range(initBatch, nowBatch)
      .map { batchID => (batchID, initV) }
      .toIterable
  }
  // Returns an iterable of all possible (Key,BatchID) combinations for
  // a given input map `m`. The type of `m` matches the return type of
  // multiOfflineGet.
  protected def generateOnlineKeys(ks: Iterable[Key], nowBatch: BatchID)
  (lookup: (Key) => Option[(BatchID,Value)])
  : Set[(Key,BatchID)] =
    pivot.invert(ks.map { k: Key => (k -> expand(lookup(k), nowBatch)) }.toMap)
      .map { pair: ((Key,BatchID),Value) => pair._1 }.toSet

  def smash(it: Iterable[(BatchID,Value)]): (BatchID,Value) = {
    implicit val vm = monoid
    Monoid.sum(it.toList.sortBy { _._1 })
  }
}

// Online-Only sinks must specify the number of batches for which
// they'll be responsible. This number is supplied by the
// BatchesToKeep case class:

case class BatchesToKeep(count: Int)

// Back to the OnlineSink. The increment function uses its time
// argument along with the batcher provided in the BaseSink to bucket
// this specific key-value pair into a batch. By partitioning a
// particular key out into batches, the Summingbird client can merge
// online values into the value stored in the OfflineSink sink
// implementation without worrying about double counts of any
// information.

// ## Compound Sink

//  A CompoundSink[Time,Key,Value] with no Online or Offline
//  qualification is a compound sink that mixes together the notions
//  of an online and offline sink:

trait CompoundSink[Time,Key,Value] extends OnlineSink[Time,Key,Value] with OfflineSink[Time,Key,Value] {
  // ## Default Implementations for extender.

  // A minimal OnlineSink must implement `getOnline`, and a minimal
  // OfflineSink must implement `getOffline`. (Sinks that wrap Finagle
  // stores might benefit from plugging in more optimal
  // implementations of `multiGetOffline` and `multiGetOnline`.)

  def get(k: Key): Future[Option[Value]] = single(k) { multiGet(_) }

  def multiGet(ks: Set[Key]): Future[Map[Key,Value]] = {
    implicit val vm = monoid
    for (offlineResult <- multiGetOffline(ks); // already packed.
         onlineKeys = generateOnlineKeys(ks, batcher.currentBatch) { offlineResult.get(_) };
         onlineRet <- multiGetOnline(onlineKeys); // unpacked!
         onlineResult = pivot(onlineRet) mapValues { smash(_) })
    yield Monoid.plus(offlineResult, onlineResult) mapValues { _._2 }
  }
}

// The following object provides implicit conversions from Offline and
// Online sinks to the more general Sink.

object CompoundSink {
  // These implicits call directly through to the overloaded "apply"
  // method because
  // 1) We need to have implicits from {online,offline} => CompoundSink
  // 2) We wanted to have the 1-arity version of "apply" accept either online or offline
  // 3) overloaded implicit methods (two implicit "apply"s, for example) aren't allowed.

  implicit def fromOnline[Time,Key,Value]
  (sink: OnlineSink[Time,Key,Value]): CompoundSink[Time,Key,Value] = apply(sink)

  implicit def fromOffline[Time,Key,Value]
  (sink: OfflineSink[Time,Key,Value]): CompoundSink[Time,Key,Value] = apply(sink)

  def apply[Time,Key,Value](offlineSink: OfflineSink[Time,Key,Value],
                            onlineSink: OnlineSink[Time,Key,Value]): CompoundSink[Time,Key,Value] = {
    assert(offlineSink.batcher == onlineSink.batcher, "Offline and Online Sinks must provide the same batcher.")
    assert(offlineSink.monoid == onlineSink.monoid, "Offline and Online Sinks must provide the same monoid.")
    new CompoundSink[Time,Key,Value] {
      override def getOffline(k: Key) = offlineSink.getOffline(k)
      override def multiGetOffline(ks: Set[Key]) = offlineSink.multiGetOffline(ks)
      override def getOnline(k: (Key,BatchID)) = onlineSink.getOnline(k)
      override def multiGetOnline(ks: Set[(Key,BatchID)]) = onlineSink.multiGetOnline(ks)
      override def batchesToKeep = onlineSink.batchesToKeep
      override def batcher = onlineSink.batcher
      override def monoid = onlineSink.monoid
      override def increment(pair: ((Key,BatchID),Value)) = { onlineSink.increment(pair) }
      override def decoderParallelism = onlineSink.decoderParallelism
      override def rpcParallelism = onlineSink.rpcParallelism
      override def sinkParallelism = onlineSink.sinkParallelism

      override def readLatest(env: ScaldingEnv)(implicit fd: FlowDef, mode: Mode) =
        offlineSink.readLatest(env)

      override def write(env: ScaldingEnv, p: TypedPipe[(Key,Value)])(implicit fd: FlowDef, mode: Mode)
      { offlineSink.write(env,p) }

      override def commit(bid: BatchID, env: ScaldingEnv) { offlineSink.commit(bid,env) }
    }
  }

  def apply[Time,Key,Value](onlineSink: OnlineSink[Time,Key,Value]): CompoundSink[Time,Key,Value] = {
    val offlineSink = new EmptyOfflineSink[Time,Key,Value](onlineSink.batcher, onlineSink.monoid)
    apply(offlineSink, onlineSink)
  }

  def apply[Time,Key,Value](offlineSink: OfflineSink[Time,Key,Value]): CompoundSink[Time,Key,Value] = {
    val onlineSink = new EmptyOnlineSink[Time,Key,Value](offlineSink.batcher, offlineSink.monoid)
    apply(offlineSink, onlineSink)
  }
}
