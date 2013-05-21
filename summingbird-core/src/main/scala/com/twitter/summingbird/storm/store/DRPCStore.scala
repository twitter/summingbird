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

package com.twitter.summingbird.storm.store

import backtype.storm.utils.DRPCClient
import com.twitter.bijection.{Injection, Conversion}
import com.twitter.util.{ Future, FuturePool }
import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.util.RpcInjection
import java.util.concurrent.Executors

import Conversion.asMethod // enable "as" syntax

/**
 * Wrapper over backtype.storm.utils.DRPCClient.
 * This ReadableStore allows the user to perform online read-only
 * queries through Storm's DRPC mechanism.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// TODO: Move to storehaus-storm

object DRPCStore {
  def apply[Key, Value](nimbusHost: String, appID: String, port: Int = 3772)
  (implicit keyCodec: Injection[Key, Array[Byte]], valCodec: Injection[Value, Array[Byte]]) =
    new DRPCStore[Key,Value](nimbusHost, appID, port)
}

class DRPCStore[Key, Value](nimbusHost: String, appID: String, port: Int)
(implicit kInjection: Injection[Key, Array[Byte]], vInjection: Injection[Value, Array[Byte]])
extends ReadableStore[(Key, BatchID), Value] {
  val futurePool = FuturePool(Executors.newFixedThreadPool(4))

  val drpcClient = new DRPCClient(nimbusHost, port)

  implicit val pair2String: Injection[(Key, BatchID), String] = RpcInjection.batchPair[Key]
  implicit val val2String: Injection[Option[Value], String] = RpcInjection.option[Value]

  override def get(pair: (Key, BatchID)): Future[Option[Value]] =
    futurePool { drpcClient.execute(appID, pair.as[String]) }
      .map { val2String.invert(_).get }
}
