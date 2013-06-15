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

package com.twitter.summingbird.example

import backtype.storm.Config
import com.twitter.bijection.AbstractInjection
import com.twitter.bijection.Bijection
import com.twitter.bijection.Base64String
import com.twitter.bijection.Bufferable
import com.twitter.bijection.Injection
import com.twitter.bijection.SwapBijection
import com.twitter.bijection.netty.Implicits._
import com.twitter.chill.KryoInjection
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.KetamaClientBuilder
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.storehaus.Store
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.memcache.HashEncoder
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.storm.{ MergeableStoreSupplier, Storm }
import twitter4j._
import twitter4j.conf.ConfigurationBuilder
import com.twitter.storehaus.memcache.MemcacheStore
import com.twitter.summingbird._
import java.net.InetSocketAddress
import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffer

object Util {
  lazy val config = new ConfigurationBuilder()
    .setOAuthConsumerKey("consumerkey")
    .setOAuthConsumerSecret("consumersecret")
    .setOAuthAccessToken("token")
    .setOAuthAccessTokenSecret("secret")
    .build

  lazy val streamFactory = new TwitterStreamFactory(config)

  val DEFAULT_TIMEOUT = 1.seconds

  def client = {
    val builder = ClientBuilder()
      .name("memcached")
      .retries(2)
      .tcpConnectTimeout(DEFAULT_TIMEOUT)
      .requestTimeout(DEFAULT_TIMEOUT)
      .connectTimeout(DEFAULT_TIMEOUT)
      .readerIdleTimeout(DEFAULT_TIMEOUT)
      .hostConnectionLimit(1)
      .codec(Memcached())

    KetamaClientBuilder()
      .clientBuilder(builder)
      .nodes("localhost:11211")
      .build()
  }

  /**
   * Returns a function that encodes a key to a Memcache key string given a
   * unique namespace string.
   */
  def keyEncoder[T](namespace: String)(implicit inj: Injection[T, Array[Byte]]): T => String = { key: T =>
    def concat(bytes: Array[Byte]): Array[Byte] = {
      namespace.getBytes ++ bytes
    }

    (inj andThen (concat _) andThen HashEncoder() andThen Bijection.connect[Array[Byte], Base64String])(key).str
  }

  def store[K, V](keyPrefix: String)
    (implicit
      kInjection: Injection[K, Array[Byte]],
      vInjection: Injection[V, Array[Byte]]): Store[K, V] = {
    implicit val valueToBuf = Injection.connect[V, Array[Byte], ChannelBuffer]
    MemcacheStore(client)
      .convert(keyEncoder[K](keyPrefix))
  }
}

object Serialization {
  implicit val timeOf: TimeExtractor[Status] =
    TimeExtractor(_.getCreatedAt.getTime)

  // Injection for our Status object.
  implicit val kryoInjection: Injection[Status, Array[Byte]] =
    new AbstractInjection[Status, Array[Byte]] {
      // Without asInstanceOf[AnyRef], I compile in 2.9.2, but I get
      // an infinite loop.
      override def apply(t: Status) = KryoInjection(t.asInstanceOf[AnyRef])
      override def invert(bytes: Array[Byte]) = KryoInjection.invert(bytes).asInstanceOf[Option[Status]]
    }

  implicit def kInjection[T](implicit inj: Injection[T, Array[Byte]]): Injection[(T, BatchID), Array[Byte]] = {
    implicit val buf = Bufferable.viaInjection[(T, BatchID), (Array[Byte], Array[Byte])]
    Bufferable.injectionOf[(T, BatchID)]
  }


  implicit def vInj[V](implicit inj: Injection[V, Array[Byte]]): Injection[(BatchID, V), Array[Byte]] =
    Injection.connect[(BatchID, V), (V, BatchID), Array[Byte]]
}

/**
  {{{
  // The following commands will look up words.

  import com.twitter.summingbird.example._
  import com.twitter.util.Await
  val myStore = StatusStreamer.mergeableStore

  def lookup(word: String) =
    Await.result(myStore.get((word, StatusStreamer.batcher.currentBatch)))
  }}}
  */
object StatusStreamer {
  import Serialization._

  def tokenize(text: String) : TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")

  /**
    * The actual Summingbird job.
    */
  def wordCountJob[P <: Platform[P]](source: Producer[P, Status], store: P#Store[String, Long]) =
    source
      .filter(_.getText != null)
      .flatMap { tweet: Status => tokenize(tweet.getText).map(_ -> 1L) }
      .sumByKey(store)


  val batcher = Batcher.ofHours(1)

  def mergeableStore: MergeableStore[(String, BatchID), Long] =
    MergeableStore.fromStore(
      Util.store[(String, BatchID), Long]("urlCount")
    )

  def buildTopology = {
    new Storm("wordCountJob", Map.empty).buildTopology {
      wordCountJob[Storm](
        Storm.source(TwitterSpout(Util.streamFactory)),
        MergeableStoreSupplier[String, Long](() => mergeableStore, batcher)
      )
    }
  }

  /**
    * Make sure to start a local memcached instance with
    * "memcached". ("brew install memcached" if you don't already have
    * it installed.)
    */
  def main(args: Array[String]) {
    import backtype.storm.LocalCluster
    val topo = StatusStreamer.buildTopology
    val cluster = new LocalCluster
    val config = new Storm("wordCountJob", Map.empty).baseConfig
    cluster.submitTopology("wordCountJob", config, topo)
  }
}
