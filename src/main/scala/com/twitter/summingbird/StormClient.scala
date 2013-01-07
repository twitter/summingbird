package com.twitter.summingbird

import com.twitter.bijection.Bijection
import backtype.storm.utils.DRPCClient
import com.twitter.summingbird.util.RpcBijectionWrapper

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Wrapper over backtype.storm.utils.DRPCClient.
 * This Client allows the user to query by the key type
 * and recieve the value type directly rather than having
 * to work with strings. Summingbird takes care of conversion
 * through bijections from Value -> String.
 */

class StormClient[Key,Value](nimbusHost: String, port: Int = 3772)
(implicit keyCodec: Bijection[Key,Array[Byte]],
 valCodec: Bijection[Value,Array[Byte]]) {
  import RpcBijectionWrapper.wrapBijection

  val drpcClient = new DRPCClient(nimbusHost, port)

  val rpcKeyCodec = wrapBijection(keyCodec)
  val rpcValCodec = wrapBijection(valCodec)

  def get(appID: String, k: Key): Value = {
    val retString = drpcClient.execute(appID, rpcKeyCodec(k))
    rpcValCodec.invert(retString)
  }
}
