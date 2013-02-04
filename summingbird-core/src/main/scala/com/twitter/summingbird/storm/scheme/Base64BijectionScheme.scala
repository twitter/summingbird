package com.twitter.summingbird.storm.scheme

import backtype.storm.tuple.{ Fields, Values }
import backtype.storm.spout.Scheme
import com.twitter.bijection.{ Base64String, Bijection }
import com.twitter.tormenta.scheme.BijectionScheme

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object Base64BijectionScheme {
  import Bijection.connect
  implicit val unwrap = Base64String.unwrap

  def apply[T](implicit bij: Bijection[T,Array[Byte]]): BijectionScheme[T] = {
    val composed = connect[T, Array[Byte], Base64String, String, Array[Byte]]
    BijectionScheme(composed)
  }
}
