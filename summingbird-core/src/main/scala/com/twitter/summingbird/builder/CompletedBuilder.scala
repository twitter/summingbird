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

package com.twitter.summingbird.builder

import backtype.storm.drpc.ReturnResults
import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import com.twitter.algebird.{ Monoid, Semigroup, SummingQueue }
import com.twitter.bijection.{Bijection, Injection, ImplicitBijection}
import com.twitter.chill.{ InjectionPair, MeatLocker }
import com.twitter.scalding.{ Job => ScaldingJob }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.{ Constants, Env }
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.{ ScaldingEnv, BatchAggregatorJob }
import com.twitter.summingbird.scalding.store.IntermediateStore
import com.twitter.summingbird.store.CompoundStore
import com.twitter.summingbird.storm.{ DecoderBolt, StormEnv, SinkBolt }
import com.twitter.summingbird.util.{ CacheSize, RpcInjection }
import com.twitter.tormenta.ScalaInterop

import MergeableStore.enrich

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// The completed builder knows about all event sources, the
// flatmapper, the sink and the DRPC configuration. At this stage
// summingbird is ready to generate scalding and storm
// implementations.

object CompletedBuilder {
  def injectionPair[T: Manifest](injection: Injection[T, Array[Byte]]) =
    InjectionPair(manifest[T].erasure.asInstanceOf[Class[T]], injection)
}

class CompletedBuilder[Key: Manifest: Ordering, Value: Manifest: Monoid](
  val flatMappedBuilder: FlatMappedBuilder[Key,Value],
  @transient tempStore: CompoundStore[Key, Value],
  @transient keyCodec: Injection[Key,Array[Byte]],
  @transient valCodec: Injection[Value,Array[Byte]],
  val batcher: Batcher,
  sinkCacheSize: CacheSize = Constants.DEFAULT_SINK_CACHE,
  sinkParallelism: SinkParallelism = Constants.DEFAULT_SINK_PARALLELISM,
  decoderParallelism: DecoderParallelism = Constants.DEFAULT_DECODER_PARALLELISM,
  rpcParallelism: RpcParallelism = Constants.DEFAULT_RPC_PARALLELISM,
  successHandler: OnlineSuccessHandler = Constants.DEFAULT_ONLINE_SUCCESS_HANDLER,
  exceptionHandler: OnlineExceptionHandler = Constants.DEFAULT_ONLINE_EXCEPTION_HANDLER,
  sinkMetrics: SinkStormMetrics = Constants.DEFAULT_SINK_STORM_METRICS,
  monoidIsCommutative: MonoidIsCommutative = Constants.DEFAULT_MONOID_IS_COMMUTATIVE,
  val intermediateStore: StoreIntermediateData[Key, Value] = StoreIntermediateData[Key, Value](None),
  maxWaitingFutures: MaxWaitingFutures = Constants.DEFAULT_MAX_WAITING_FUTURES,
  includeSuccessHandler: IncludeSuccessHandler = IncludeSuccessHandler(true)
) extends java.io.Serializable {
  import CompletedBuilder.injectionPair

  protected val storeBox = MeatLocker(tempStore)
  def store = storeBox.copy
  def monoid: Monoid[Value] = implicitly[Monoid[Value]]

  val keyCodecPair = injectionPair(keyCodec)
  val valueCodecPair = injectionPair(valCodec)

  /**
   * Use this with named params for easy copying.
   */
  def copy(
    fmb: FlatMappedBuilder[Key, Value] = flatMappedBuilder,
    store: CompoundStore[Key, Value] = tempStore,
    kInjection: Injection[Key,Array[Byte]] = keyCodec,
    vInjection: Injection[Value,Array[Byte]] = valCodec,
    longBatcher: Batcher = batcher,
    cacheSize: CacheSize = sinkCacheSize,
    sinkPar: SinkParallelism = sinkParallelism,
    decoderPar: DecoderParallelism = decoderParallelism,
    rpcPar: RpcParallelism = rpcParallelism,
    succHandler: OnlineSuccessHandler = successHandler,
    excHandler: OnlineExceptionHandler = exceptionHandler,
    metrics: SinkStormMetrics = sinkMetrics,
    commutative: MonoidIsCommutative = monoidIsCommutative,
    intermediate: StoreIntermediateData[Key, Value] = intermediateStore,
    futures: MaxWaitingFutures = maxWaitingFutures,
    includeHandler: IncludeSuccessHandler = includeSuccessHandler
  ): CompletedBuilder[Key, Value] =
    new CompletedBuilder(fmb, store, kInjection, vInjection, longBatcher,
      cacheSize, sinkPar, decoderPar, rpcPar,
      succHandler, excHandler, metrics, commutative, intermediate, futures, includeHandler)

  // Set the cache size used in the online flatmap step.
  def set(size: CacheSize)(implicit env: Env) = {
    val cb = copy(cacheSize = size)
    env.builder = cb
    cb
  }

  def set(opt: SinkOption)(implicit env: Env) = {
    val cb = opt match {
      case par: SinkParallelism => copy(sinkPar = par)
      case par: DecoderParallelism => copy(decoderPar = par)
      case par: RpcParallelism => copy(rpcPar = par)
      case handler: OnlineSuccessHandler => copy(succHandler = handler)
      case handler: OnlineExceptionHandler => copy(excHandler = handler)
      case newMetrics: SinkStormMetrics => copy(metrics = newMetrics)
      case comm: MonoidIsCommutative => copy(commutative = comm)
      case inter: StoreIntermediateData[Key, Value] => copy(intermediate = inter)
      case futures: MaxWaitingFutures => copy(futures = futures)
      case includeHander: IncludeSuccessHandler => copy(includeHandler = includeHander)
    }
    env.builder = cb
    cb
  }

  // In addition to supporting Summingbird's source->flatmap->sink
  // processing pipeline, the Storm topology generated here also
  // handles DRPC requests into that topology. A Summingbird store
  // wraps a random-write, random-read realtime data store; the DRPC
  // support allows topologies that store data completely in-memory in
  // the Storm SinkBolt.
  def buildStorm(env : StormEnv) : StormTopology = {
    import Constants._

    val topologyBuilder = new TopologyBuilder

    // Add the DRPC spout to the topology. env.jobName is used by the
    // Summingbird Client to make the DRPC call.
    topologyBuilder.setSpout(DRPC_SPOUT, ScalaInterop.makeDRPC(env.jobName))

    // add the EventSource spout and flatMapping bolt
    flatMappedBuilder.addToTopo(env, topologyBuilder, "-root")

    // The DecoderBolt accepts DRPC requests, decodes them from String
    // -> Key and emits the decoded Key. The SinkBolt uses this stream
    // to return Values for DRPC requests.
    topologyBuilder.setBolt(DRPC_DECODER,
                            new DecoderBolt(RpcInjection.batchPair(keyCodec)),
                            decoderParallelism.parHint)
      .shuffleGrouping(DRPC_SPOUT)

    // Attach the SinkBolt, used to route key-value pairs into the
    // Summingbird sink and to look up values for DRPC requests.
    val sinkBolt = new SinkBolt[Key, Value](
      // TODO: look into adding back the withSummer logic and removing
      // the cache inside the sink.

      // the fix must have appropriate behavior related to the
      // FutureQueue
      { () => store.onlineSupplier() },
      successHandler, exceptionHandler, sinkCacheSize, sinkMetrics, maxWaitingFutures,
      includeSuccessHandler
    )

    flatMappedBuilder.attach(
      topologyBuilder.setBolt(GROUP_BY_SUM, sinkBolt, sinkParallelism.parHint)
        .fieldsGrouping(DRPC_DECODER, new Fields(AGG_KEY)),
      "-root")

    // DRPC return bolt provided by storm.
    topologyBuilder.setBolt(RPC_RETURN, new ReturnResults, rpcParallelism.parHint)
      .shuffleGrouping(GROUP_BY_SUM, RPC_STREAM)

    topologyBuilder.createTopology
  }

  def buildScalding(env : ScaldingEnv) : ScaldingJob =
    new BatchAggregatorJob[Key, Value](
      flatMappedBuilder, env, batcher, store.offlineStore,
      monoidIsCommutative.isCommutative,
      intermediateStore.store
    )
}
