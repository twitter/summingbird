package com.twitter.summingbird.util

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.util.Duration

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
*/

object FinagleUtil {
  def clientBuilder(name: String, retries: Int, timeout: Duration) = {
    ClientBuilder()
      .name(name + "_client")
      .retries(retries)
      .tcpConnectTimeout(timeout)
      .requestTimeout(timeout)
      .connectTimeout(timeout)
      .readerIdleTimeout(timeout)
  }
}
