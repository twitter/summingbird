package com.twitter.summingbird.store

import com.twitter.bijection.Bijection
import com.twitter.conversions.time._
import com.twitter.util.{ Duration, Encoder, Future, FuturePool, Time }
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.{ Client, KetamaClientBuilder }
import com.twitter.summingbird.bijection.HashEncoder
import com.twitter.summingbird.util.{ FinagleUtil, FutureUtil }

import FutureUtil._

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// "weight" is a finagle implementation detail for the Memcached
// client. A higher weight gives a particular host precedence over
// others of lower weight in a client instance's host list.

case class HostConfig(host: String, port: Int = 11211, weight: Int = 1) {
  def toTuple = (host, port, weight)
}

object MemcacheStore {
  // Default Memcached TTL is one day.
  val DEFAULT_TTL = Time.fromSeconds(24 * 60 * 60)

  // implicitly convert the standard key serialization bijection into a
  // key->hashed string bijection, suitable for use with Memcached.
  implicit def toEncoder[Key](implicit bijection: Bijection[Key,Array[Byte]])
  : Encoder[Key,String] =
    new Encoder[Key,String] {
      val enc = (bijection andThen HashEncoder() andThen Bijection.bytes2Base64)
      override def encode(k: Key) = enc(k).str
    }

  // Instantiate a Memcached store using a Ketama client with the
  // given # of retries, request timeout and ttl.
  def apply[Key,Value](hosts: Seq[HostConfig],
                       retries: Int = 2,
                       timeout: Duration = 1.seconds,
                       ttl: Time = DEFAULT_TTL)
  (implicit encoder: Encoder[Key,String], bijection: Bijection[Value,Array[Byte]]) =
    new MemcacheStore[Key,Value](hosts, encoder, bijection, retries, timeout, ttl)
}

class MemcacheStore[Key,Value](hosts: Seq[HostConfig],
                               enc: Encoder[Key,String],
                               bijection: Bijection[Value,Array[Byte]],
                               retries: Int,
                               timeout: Duration,
                               ttl: Time)
extends ConcurrentMutableStore[MemcacheStore[Key,Value],Key,Value] {
  // Memcache flag used on "set" operations. Search this page for
  // "flag" for more info on the following variable:
  // http://docs.libmemcached.org/memcached_set.html

  val MEMCACHE_FLAG = 0

  lazy val client = FuturePool.unboundedPool {
    val builder = FinagleUtil.clientBuilder("summingbird", retries, timeout)
      .hostConnectionLimit(1)
      .codec(Memcached())
    KetamaClientBuilder()
      .clientBuilder(builder)
      .nodes(hosts.map(_.toTuple))
      .build()
      .withBytes // Adaptor to allow bytes vs channel buffers.
  }

  override def get(k: Key): Future[Option[Value]] =
    client flatMap { c =>
      c.get(enc(k)) map { opt =>
        opt map { bijection.invert(_) }
      }
    }

  override def multiGet(ks: Set[Key]): Future[Map[Key,Value]] = {
    val encodedKs = ks map { enc(_) }
    val encMap = (encodedKs zip ks).toMap

    client flatMap { c =>
      c.get(encodedKs.toIterable) map { m =>
        m map { case (k,v) => encMap(k) -> bijection.invert(v) }
      }
    }
  }

  override def -(k: Key) = client flatMap { _.delete(enc(k)) map { _ => this } }
  override def +(pair: (Key, Value)) = set(pair._1, pair._2) map { _ => this }

  protected def set(k: Key, v: Value) = {
    val valBytes = bijection(v)
    client flatMap { _.set(enc(k), MEMCACHE_FLAG, ttl, valBytes) }
  }
}
