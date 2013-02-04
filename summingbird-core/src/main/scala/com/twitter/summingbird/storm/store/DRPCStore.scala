package com.twitter.summingbird.storm.store

import backtype.storm.utils.DRPCClient
import com.twitter.bijection.Bijection
import com.twitter.util.{ Future, FuturePool }
import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.util.RpcBijection
import java.util.concurrent.Executors

import Bijection.asMethod // enable "as" syntax

/**
 * Wrapper over backtype.storm.utils.DRPCClient.
 * This ReadableStore allows the user to perform online read-only
 * queries through Storm's DRPC mechanism.
 *
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// TODO: Move to storehaus-storm

object DRPCStore {
  def apply[Key, Value](nimbusHost: String, appID: String, port: Int = 3772)
  (implicit keyCodec: Bijection[Key, Array[Byte]], valCodec: Bijection[Value, Array[Byte]]) =
    new DRPCStore[Key,Value](nimbusHost, appID, port)
}

class DRPCStore[Key, Value](nimbusHost: String, appID: String, port: Int)
(implicit kBijection: Bijection[Key, Array[Byte]], vBijection: Bijection[Value, Array[Byte]])
extends ReadableStore[(Key, BatchID), Value] {
  val futurePool = FuturePool(Executors.newFixedThreadPool(4))

  val drpcClient = new DRPCClient(nimbusHost, port)

  implicit val pair2String: Bijection[(Key, BatchID), String] = RpcBijection.batchPair[Key]
  implicit val val2String: Bijection[Option[Value], String] = RpcBijection.option[Value]

  override def get(pair: (Key, BatchID)): Future[Option[Value]] =
    futurePool { drpcClient.execute(appID, pair.as[String]) }
      .map { _.as[Option[Value]] }
}
