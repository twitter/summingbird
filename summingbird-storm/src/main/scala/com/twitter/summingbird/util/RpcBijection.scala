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

package com.twitter.summingbird.util

import com.twitter.bijection.{ Base64String, Bufferable, Injection }
import com.twitter.summingbird.batch.BatchID

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// TODO: We might be able to handle this with bijection-json.
object RpcInjection {
  import Injection.connect
  import Bufferable.{ injectionOf, viaInjection }

  def of[T](implicit bijection: Injection[T, Array[Byte]])
  : Injection[T, String] =
    connect[T, Array[Byte], Base64String, String]

  def batchPair[K](implicit bijection: Injection[K, Array[Byte]])
  : Injection[(K, BatchID), String] = {
    val SEP = ":"

    implicit val pairInjection: Injection[(String, String), String] =
      new Injection[(String, String), String] {
        override def apply(pair: (String, String)) = pair._1 + SEP + pair._2
        override def invert(s: String) = {
          val parts = s.split(SEP)
          if (parts.size == 2) {
            Some((parts(0), parts(1)))
          }
          else {
            None
          }
        }
      }
    implicit val kInjection: Injection[K, String] = of[K]

    connect[(K, BatchID), (String, String), String]
  }

  def option[V](implicit injection: Injection[V, Array[Byte]])
  : Injection[Option[V], String] = {
    implicit val vBuf: Bufferable[V] = viaInjection[V, Array[Byte]]
    implicit val optBij: Injection[Option[V], Array[Byte]] = injectionOf[Option[V]]
    connect[Option[V], Array[Byte], Base64String, String]
  }
}
