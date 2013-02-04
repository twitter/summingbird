/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
