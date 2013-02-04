package com.twitter.summingbird.scalding

import com.esotericsoftware.kryo.Kryo
import com.twitter.scalding.serialization.KryoHadoop
import com.twitter.summingbird.util.KryoRegistrationHelper
import com.twitter.summingbird.scalding.ConfigBijection.fromJavaMap

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class SummingbirdKryoHadoop extends KryoHadoop {
  import KryoRegistrationHelper._

  override def decorateKryo(newK: Kryo) {
    super.decorateKryo(newK)
    val m = fromJavaMap.invert(getConf)
    registerBijections(newK, m)
    registerBijectionDefaults(newK, m)
    registerKryoClasses(newK, m)
  }
}
