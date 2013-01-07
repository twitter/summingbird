package com.twitter.summingbird.sink

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// SinkParallelism controls the number of executors storm allocates to
// the groupAndSum bolts. Each of these bolt executors is responsible
// for storing and committing some subset of the keyspace to the
// Sink's store, so higher parallelism will result in higher load on
// the store.
//
// The default sink parallelism is 5. To override, add something like
// this to your AbstractJob extension:
//
// implicit val sinkPar = SinkParallelism(10)

object SinkParallelism {
  implicit val sp = SinkParallelism(5)
}

case class SinkParallelism(parHint: Int) extends java.io.Serializable

// RpcParallelism controls the number of processes Storm allocates to
// the Rpc Return bolts. The default rpc parallelism is 10. To
// override, add something like this to your AbstractJob extension:
//
// implicit val rpcPar = RpcParallelism(20)

object RpcParallelism {
  implicit val dp = RpcParallelism(10)
}

case class RpcParallelism(parHint: Int) extends java.io.Serializable

// DecoderParallelism controls the number of processes Storm allocates
// to the Decoder bolts. The decoder bolts recieve DRPC requests,
// decode them into a Key instance and pass this key along to the sink
// for lookup and return (via the ReturnResults bolt).
//
// The default decoder parallelism is 10. To override, add something
// like this to your AbstractJob extension:
//
// implicit val rpcPar = RpcParallelism(20)

object DecoderParallelism {
  implicit val dp = DecoderParallelism(10)
}

case class DecoderParallelism(parHint: Int) extends java.io.Serializable
