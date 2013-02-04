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
