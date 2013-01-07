package com.twitter.summingbird.builder

import backtype.storm.drpc.ReturnResults
import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import com.twitter.bijection.Bijection
import com.twitter.chill.{ BijectionPair, MeatLocker }
import com.twitter.scalding.{ Job => ScaldingJob }
import com.twitter.summingbird.{ Constants, Env }
import com.twitter.summingbird.scalding.{ ScaldingEnv, BatchAggregatorJob }
import com.twitter.summingbird.storm.{ DecoderBolt, StormEnv, SinkBolt }
import com.twitter.summingbird.sink.CompoundSink
import com.twitter.summingbird.util.RpcBijectionWrapper
import com.twitter.tormenta.ScalaInterop

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// The completed builder knows about all event sources, the
// flatmapper, the sink and the DRPC configuration. At this stage
// summingbird is ready to generate scalding and storm
// implementations.

class CompletedBuilder[Time,Key,Value](val flatMappedBuilder: FlatMappedBuilder[Time,Key,Value],
                                       @transient tempSink: CompoundSink[Time,Key,Value],
                                       @transient keyCodec: Bijection[Key,Array[Byte]],
                                       @transient valCodec: Bijection[Value,Array[Byte]]) extends java.io.Serializable {
  import Constants._
  import RpcBijectionWrapper.wrapBijection

  protected val sinkBox = MeatLocker(tempSink)
  def sink = sinkBox.copy

  val keyCodecPair   = BijectionPair(flatMappedBuilder.keyClass, keyCodec)
  val valueCodecPair = BijectionPair(flatMappedBuilder.valueClass, valCodec)

  // In addition to supporting Summingbird's source->flatmap->sink
  // processing pipeline, the Storm topology generated here also
  // handles DRPC requests into that topology. A Summingbird sink
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
                            new DecoderBolt(wrapBijection(keyCodec)),
                            sink.decoderParallelism.parHint)
      .shuffleGrouping(DRPC_SPOUT)

    // Attach the SinkBolt, used to route key-value pairs into the
    // Summingbird sink and to look up values for DRPC requests.
    flatMappedBuilder.attach(topologyBuilder.setBolt(GROUP_BY_SUM,
                                                     new SinkBolt(sink, wrapBijection(valCodec)),
                                                     sink.sinkParallelism.parHint)
      .fieldsGrouping(DRPC_DECODER, new Fields(AGG_KEY)), "-root")

    // DRPC return bolt provided by storm.
    topologyBuilder.setBolt(RPC_RETURN, new ReturnResults, sink.rpcParallelism.parHint)
      .shuffleGrouping(GROUP_BY_SUM, RPC_STREAM)

    topologyBuilder.createTopology
  }

  def buildScalding(env : ScaldingEnv) : ScaldingJob = {
    new BatchAggregatorJob[Time,Key,Value](this, env)
  }
}
