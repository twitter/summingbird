package com.twitter.summingbird.util

import com.twitter.bijection.{ Base64String, Bijection }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object RpcBijectionWrapper {
  import Bijection.connect
  implicit val unwrap = Base64String.unwrap

  def wrapBijection[T](implicit bijection: Bijection[T,Array[Byte]]): Bijection[T,String] =
    connect[T, Array[Byte], Base64String, String]
}
