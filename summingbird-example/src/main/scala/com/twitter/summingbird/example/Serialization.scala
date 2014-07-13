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

import com.twitter.bijection.{ Bufferable, Codec, Injection }
import com.twitter.summingbird.batch.BatchID
import twitter4j.Status
import twitter4j.json.DataObjectFactory

/**
 * Serialization is often the most important (and hairy)
 * configuration issue for any system that needs to store its data
 * over the long term. Summingbird controls serialization through the
 * "Injection" interface.
 *
 * By maintaining identical Injections from K and V to Array[Byte],
 * one can guarantee that data written one day will be readable the
 * next. This isn't the case with serialization engines like Kryo,
 * where serialization format depends on unstable parameters, like
 * the serializer registration order for the given Kryo instance.
 */

object Serialization {
  /**
   * This Injection converts the twitter4j.Status objects that Storm
   * and Scalding will process into Strings.
   */
  implicit val statusCodec: Injection[Status, String] =
    Injection.buildCatchInvert[Status, String](DataObjectFactory.getRawJSON(_))(
      json => DataObjectFactory.createStatus(json)
    )

  /**
   * We can chain the Status <-> String injection above with the
   * library-supplied String <-> Array[Byte] injection to generate a
   * full-on serializer for Status objects of the type
   * Injection[Status, Array[Byte]]. Our Storm and Scalding sources
   * can now pull in this injection using Scala's implicit resolution
   * and properly register the serializer.
   */
  implicit val toBytes = Injection.connect[Status, String, Array[Byte]]

  /**
   * Summingbird's implementation of the batch/realtime merge
   * requires that the Storm-based workflow store (K, BatchID) -> V
   * pairs, while the Hadoop-based workflow stores K -> (BatchID, V)
   * pairs.
   *
   * The following two injections use Bijection's "Bufferable" object
   * to generate injections that take (T, BatchID) or (BatchID, T) to
   * bytes.
   *
   * For true production applications, I'd suggest defining a thrift
   * or protobuf "pair" structure that can safely store these pairs
   * over the long-term.
   */
  implicit def kInjection[T: Codec]: Injection[(T, BatchID), Array[Byte]] = {
    implicit val buf =
      Bufferable.viaInjection[(T, BatchID), (Array[Byte], Array[Byte])]
    Bufferable.injectionOf[(T, BatchID)]
  }

  implicit def vInj[V: Codec]: Injection[(BatchID, V), Array[Byte]] =
    Injection.connect[(BatchID, V), (V, BatchID), Array[Byte]]
}
