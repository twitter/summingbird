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

package com.twitter.summingbird.builder

import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.{ Env, KeyedProducer, Options }
import com.twitter.summingbird.scalding.Scalding
import com.twitter.summingbird.storm.Storm
import com.twitter.summingbird.util.CacheSize

import java.io.Serializable

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object CompletedBuilder {
  def injectionPair[T: Manifest](injection: Injection[T, Array[Byte]]) =
    InjectionPair(manifest[T].erasure.asInstanceOf[Class[T]], injection)
}

/**
  * Required for compatibility between the new and old APIs. The old
  * API stored job state inside of the Scalding store. The new API
  * breaks out a State abstraction; because we can't pull a path out
  * of the store itself, the user needs to supply an implicit
  * StatePath. For migrating jobs, the StatePath will be the same as
  * the HDFS path storing the scalding key-value pairs:

  {{{
  implicit val statePath = StatePath("/user/sritchie/mydata")
  }}}

  */
case class StatePath(path: String)

case class CompletedBuilder[K: Manifest, V: Manifest](
  node: SourceBuilder.SummerNode[K, V],
  eventCodecPairs: List[InjectionPair[_]],
  batcher: Batcher,
  statePath: StatePath,
  @transient keyCodec: Injection[K, Array[Byte]],
  @transient valCodec: Injection[V, Array[Byte]],
  id: String,
  opts: Map[String, Options]) extends Serializable {
  import SourceBuilder.adjust
  import CompletedBuilder.injectionPair

  val keyCodecPair = injectionPair(keyCodec)
  val valueCodecPair = injectionPair(valCodec)

  // Set the cache size used in the online flatmap step.
  def set(size: CacheSize)(implicit env: Env) = {
    val cb = copy(opts = adjust(opts, id)(_.set(size)))
    env.builder = cb
    cb
  }

  def set(opt: SinkOption)(implicit env: Env) = {
    val cb = copy(opts = adjust(opts, id)(_.set(opt)))
    env.builder = cb
    cb
  }
}
