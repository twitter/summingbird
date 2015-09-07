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
import com.twitter.chill.{ InjectionDefaultRegistrar, InjectionRegistrar, IKryoRegistrar }
import com.twitter.chill.java.IterableRegistrar
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird.{ Env, KeyedProducer, Options, Platform, Summer }
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.scalding.Scalding
import com.twitter.summingbird.storm.Storm

import java.io.Serializable

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object CompletedBuilder {
  def injectionRegistrar[T: Manifest](injection: Injection[T, Array[Byte]]) =
    InjectionRegistrar(manifest[T].runtimeClass.asInstanceOf[Class[T]], injection)

  def injectionDefaultRegistrar[T: Manifest](injection: Injection[T, Array[Byte]]) =
    InjectionDefaultRegistrar(manifest[T].runtimeClass.asInstanceOf[Class[T]], injection)
}

case class CompletedBuilder[P <: Platform[P], K, V](
    @transient node: Summer[P, K, V],
    @transient eventRegistrar: IKryoRegistrar,
    @transient batcher: Batcher,
    @transient keyCodec: Injection[K, Array[Byte]],
    @transient valCodec: Injection[V, Array[Byte]],
    id: String,
    @transient opts: Map[String, Options])(implicit val keyMf: Manifest[K], val valMf: Manifest[V]) extends Serializable {
  import SourceBuilder.adjust
  import CompletedBuilder._

  @transient val registrar =
    new IterableRegistrar(
      eventRegistrar,
      injectionDefaultRegistrar(keyCodec),
      injectionDefaultRegistrar(valCodec)
    )

  // Set any Option
  def set[T](opt: T)(implicit env: Env) = {
    val cb = copy(opts = adjust(opts, id)(_.set(opt)))
    env.builder = cb
    cb
  }
}
