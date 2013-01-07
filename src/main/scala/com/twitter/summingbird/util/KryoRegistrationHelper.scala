package com.twitter.summingbird.util

import com.esotericsoftware.kryo.Kryo
import com.twitter.bijection.{ Base64String, Bijection, CastBijection }
import com.twitter.chill.{ BijectionPair, KryoBijection, KryoSerializer }
import com.twitter.summingbird.bijection.BijectionImplicits
import com.twitter.tormenta.serialization.ScalaKryoFactory

import java.util.{ HashMap, Map => JMap }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
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

  def confBijection[T]: Bijection[T, String] =
    CastBijection.of[T,AnyRef]
      .andThen(KryoBijection)
      .andThen(implicitly[Bijection[Array[Byte],Base64String]])
      .andThen(Base64String.unwrap)

  def registerBijections(conf: JMap[String,AnyRef], pairs: Iterable[BijectionPair[_]]) {
    conf.put(BIJECTION_PAIRS, confBijection(pairs))
  }

  def getRegisteredBijections(conf: JMap[_,_]): Option[Iterable[BijectionPair[_]]] =
    getConfValue[String](conf, BIJECTION_PAIRS) map { confBijection.invert(_) }

  def registerBijectionDefaults(conf: JMap[String,AnyRef], pairs: Iterable[BijectionPair[_]]) {
    conf.put(BIJECTION_DEFAULT_PAIRS, confBijection(pairs))
  }

  def getRegisteredBijectionDefaults(conf: JMap[_,_]): Option[Iterable[BijectionPair[_]]] =
    getConfValue[String](conf, BIJECTION_DEFAULT_PAIRS) map { confBijection.invert(_) }

  def registerClasses(conf: JMap[String,AnyRef], klasses: Iterable[Class[_]]) {
    conf.put(CLASS_REGISTRATIONS, confBijection(klasses))
  }

  def getRegisteredClasses(conf: JMap[_,_]): Option[Iterable[Class[_]]] =
    getConfValue[String](conf, CLASS_REGISTRATIONS) map { confBijection.invert(_) }

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
