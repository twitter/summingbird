package com.twitter.summingbird.storm

import com.esotericsoftware.kryo.Kryo
import com.twitter.summingbird.util.KryoRegistrationHelper
import com.twitter.tormenta.serialization.ScalaKryoFactory
import java.util.{ Map => JMap }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object SummingbirdKryoFactory {
  def apply = new SummingbirdKryoFactory
}

class SummingbirdKryoFactory extends ScalaKryoFactory {
  import KryoRegistrationHelper._

  override def preRegister(k: Kryo, conf: JMap[_,_]) {
    super.preRegister(k, conf)
    registerBijections(k, conf)
    registerBijectionDefaults(k, conf)
    registerKryoClasses(k, conf)
    k.setRegistrationRequired(false) // TODO: Is this necessary?
  }
}
