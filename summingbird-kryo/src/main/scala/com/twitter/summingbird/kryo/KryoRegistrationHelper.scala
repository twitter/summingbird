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

package com.twitter.summingbird.kryo

import com.esotericsoftware.kryo.Kryo
import com.twitter.bijection.CastInjection
import com.twitter.bijection.{ Base64String, Bijection, Injection }
import com.twitter.chill.{ InjectionPair, KryoInjection, KryoSerializer }

import java.util.{ HashMap, Map => JMap }

// TODO Move this to chill

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
  val SEPARATOR = ":"

  // TODO: KryoInjection should be Any -> Bytes
  val base64KryoInjection: Injection[AnyRef, String] =
    KryoInjection
      .andThen(Bijection.connect[Array[Byte],Base64String])
      .andThen(Base64StringUnwrap)

  // TODO: can we store type params in here too and prevent the cast?
  def getConfValue[T](conf: JMap[_, _], key: String): Option[T] =
    Option(conf.get(key).asInstanceOf[T])

  def getAll[T](conf: JMap[_, _], key: String): Option[Iterable[T]] =
    getConfValue[String](conf, key)
      .map(_.split(SEPARATOR))
      .map { _.flatMap(base64KryoInjection.invert(_).asInstanceOf[Option[T]]) }

  def append(conf: JMap[String, AnyRef], key: String, entry: String) {
    val newV = List(getConfValue[String](conf, key).getOrElse(""), entry)
      .mkString(SEPARATOR)
    conf.put(key, newV)
  }

  def appendAll(conf: JMap[String, AnyRef], k: String, items: TraversableOnce[AnyRef]) {
    items.foreach { item =>
      append(conf, k, base64KryoInjection(item))
    }
  }

  /**
    * Injections for specific classes.
    */
  def resetInjections(conf: JMap[String, AnyRef]) { conf.remove(INJECTION_PAIRS) }
  def registerInjections(conf: JMap[String, AnyRef], pairs: TraversableOnce[InjectionPair[_]]) {
    appendAll(conf, INJECTION_PAIRS, pairs)
  }
  def getRegisteredInjections(conf: JMap[_,_]): Option[Iterable[InjectionPair[_]]] =
    getAll(conf, INJECTION_PAIRS)

  /**
    * Injections for subclasses.
    */
  def resetInjectionDefaults(conf: JMap[_, _]) { conf.remove(INJECTION_PAIRS) }
  def registerInjectionDefaults(conf: JMap[String,AnyRef], pairs: TraversableOnce[InjectionPair[_]]) {
    appendAll(conf, INJECTION_DEFAULT_PAIRS, pairs)
  }
  def getRegisteredInjectionDefaults(conf: JMap[_,_]): Option[Iterable[InjectionPair[_]]] =
    getAll(conf, INJECTION_DEFAULT_PAIRS)

  /**
    * Registration for classes.
    */
  def resetClasses(conf: JMap[_, _]) { conf.remove(CLASS_REGISTRATIONS) }
  def registerClasses(conf: JMap[String, AnyRef], klasses: TraversableOnce[Class[_]]) {
    appendAll(conf, CLASS_REGISTRATIONS, klasses)
  }
  def getRegisteredClasses(conf: JMap[_,_]): Option[Iterable[Class[_]]] =
    getAll(conf, CLASS_REGISTRATIONS)

  /**
    * Actual Kryo registration.
    */
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
