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
import com.twitter.bijection.{ Base64String, Bijection }
import com.twitter.chill.{ InjectionPair, KryoBijection, KryoSerializer }
import com.twitter.tormenta.serialization.ScalaKryoFactory

import java.util.{ HashMap, Map => JMap }

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// unsafe. remove when commons-codec issue gets resolved.
object Base64StringUnwrap extends Bijection[Base64String, String] {
  override def apply(bs: Base64String) = bs.str
  override def invert(str: String) = Base64String(str)
}

object KryoRegistrationHelper {
  val INJECTION_PAIRS = "summingbird.injection.pairs"
  val INJECTION_DEFAULT_PAIRS = "summingbird.injection.pairs.default"
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
      .andThen(Base64StringUnwrap)

  def registerInjections(conf: JMap[String,AnyRef], pairs: Iterable[InjectionPair[_]]) {
    conf.put(INJECTION_PAIRS, base64KryoBijection(pairs))
  }

  def getRegisteredInjections(conf: JMap[_,_]): Option[Iterable[InjectionPair[_]]] =
    getConfValue[String](conf, INJECTION_PAIRS)
      .map { base64KryoBijection.invert(_).asInstanceOf[Iterable[InjectionPair[_]]] }

  def registerInjectionDefaults(conf: JMap[String,AnyRef], pairs: Iterable[InjectionPair[_]]) {
    conf.put(INJECTION_DEFAULT_PAIRS, base64KryoBijection(pairs))
  }

  def getRegisteredInjectionDefaults(conf: JMap[_,_]): Option[Iterable[InjectionPair[_]]] =
    getConfValue[String](conf, INJECTION_DEFAULT_PAIRS)
      .map { base64KryoBijection.invert(_).asInstanceOf[Iterable[InjectionPair[_]]] }

  def registerClasses(conf: JMap[String,AnyRef], klasses: Iterable[Class[_]]) {
    conf.put(CLASS_REGISTRATIONS, base64KryoBijection(klasses))
  }

  def getRegisteredClasses(conf: JMap[_,_]): Option[Iterable[Class[_]]] =
    getConfValue[String](conf, CLASS_REGISTRATIONS)
      .map { base64KryoBijection.invert(_).asInstanceOf[Iterable[Class[_]]] }

  def registerInjections(k: Kryo, conf: JMap[_,_]) {
    getRegisteredInjections(conf)
      .foreach { KryoSerializer.registerInjections(k, _) }
  }

  def registerInjectionDefaults(k: Kryo, conf: JMap[_,_]) {
    getRegisteredInjectionDefaults(conf)
      .foreach { KryoSerializer.registerInjectionDefaults(k, _) }
  }

  def registerKryoClasses(k: Kryo, conf: JMap[_,_]) {
    getRegisteredClasses(conf)
      .foreach { KryoSerializer.registerClasses(k, _) }
  }
}
