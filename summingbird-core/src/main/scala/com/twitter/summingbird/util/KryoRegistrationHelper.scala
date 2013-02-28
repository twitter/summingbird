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

import com.esotericsoftware.kryo.Kryo
import com.twitter.bijection.{ Base64String, Bijection, CastBijection }
import com.twitter.chill.{ BijectionPair, KryoBijection, KryoSerializer }
import com.twitter.tormenta.serialization.ScalaKryoFactory

import java.util.{ HashMap, Map => JMap }

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object KryoRegistrationHelper {
  val BIJECTION_PAIRS = "summingbird.bijection.pairs"
  val BIJECTION_DEFAULT_PAIRS = "summingbird.bijection.pairs.default"
  val CLASS_REGISTRATIONS = "summingbird.class.registrations"

  // TODO: can we store type params in here too and prevent the cast?
  def getConfValue[T](conf: JMap[_,_], key: String): Option[T] = {
    val ret = conf.get(key).asInstanceOf[T]
    if (ret != null)
      Some(ret)
    else
      None
  }

  val base64KryoBijection: Bijection[AnyRef, String] =
    KryoBijection
      .andThen(implicitly[Bijection[Array[Byte],Base64String]])
      .andThen(Base64String.unwrap)

  def registerBijections(conf: JMap[String,AnyRef], pairs: Iterable[BijectionPair[_]]) {
    conf.put(BIJECTION_PAIRS, base64KryoBijection(pairs))
  }

  def getRegisteredBijections(conf: JMap[_,_]): Option[Iterable[BijectionPair[_]]] =
    getConfValue[String](conf, BIJECTION_PAIRS)
      .map { base64KryoBijection.invert(_).asInstanceOf[Iterable[BijectionPair[_]]] }

  def registerBijectionDefaults(conf: JMap[String,AnyRef], pairs: Iterable[BijectionPair[_]]) {
    conf.put(BIJECTION_DEFAULT_PAIRS, base64KryoBijection(pairs))
  }

  def getRegisteredBijectionDefaults(conf: JMap[_,_]): Option[Iterable[BijectionPair[_]]] =
    getConfValue[String](conf, BIJECTION_DEFAULT_PAIRS)
      .map { base64KryoBijection.invert(_).asInstanceOf[Iterable[BijectionPair[_]]] }

  def registerClasses(conf: JMap[String,AnyRef], klasses: Iterable[Class[_]]) {
    conf.put(CLASS_REGISTRATIONS, base64KryoBijection(klasses))
  }

  def getRegisteredClasses(conf: JMap[_,_]): Option[Iterable[Class[_]]] =
    getConfValue[String](conf, CLASS_REGISTRATIONS)
      .map { base64KryoBijection.invert(_).asInstanceOf[Iterable[Class[_]]] }

  def registerBijections(k: Kryo, conf: JMap[_,_]) {
    getRegisteredBijections(conf) map { KryoSerializer.registerBijections(k, _) }
  }

  def registerBijectionDefaults(k: Kryo, conf: JMap[_,_]) {
    getRegisteredBijectionDefaults(conf) map { KryoSerializer.registerBijectionDefaults(k, _) }
  }

  def registerKryoClasses(k: Kryo, conf: JMap[_,_]) {
    getRegisteredClasses(conf) map { KryoSerializer.registerClasses(k, _) }
  }
}
