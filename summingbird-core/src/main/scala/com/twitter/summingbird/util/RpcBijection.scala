package com.twitter.summingbird.util

import com.twitter.bijection.{ Base64String, Bijection, Bufferable }
import com.twitter.summingbird.batch.BatchID

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// TODO: We might be able to handle this with bijection-json.
object RpcBijection {
  import Bijection.connect
  import Bufferable.{ bijectionOf, viaBijection }

  implicit val unwrap = Base64String.unwrap

  def of[T](implicit bijection: Bijection[T, Array[Byte]])
  : Bijection[T, String] =
    connect[T, Array[Byte], Base64String, String]

  def batchPair[K](implicit bijection: Bijection[K, Array[Byte]])
  : Bijection[(K, BatchID), String] = {
    val SEP = ":"

    implicit val pairBijection: Bijection[(String, String), String] =
      new Bijection[(String, String), String] {
        override def apply(pair: (String, String)) = pair._1 + SEP + pair._2
        override def invert(s: String) = {
          val parts = s.split(SEP)
          (parts(0), parts(1))
        }
      }
    implicit val kBijection: Bijection[K, String] = of[K]

    connect[(K, BatchID), (String, String), String]
  }

  def option[V](implicit bijection: Bijection[V, Array[Byte]])
  : Bijection[Option[V], String] = {
    implicit val vBuf: Bufferable[V] =
      viaBijection[V, Array[Byte]]
    implicit val optBij: Bijection[Option[V], Array[Byte]] =
      bijectionOf[Option[V]]
    connect[Option[V], Array[Byte], Base64String, String]
  }
}
