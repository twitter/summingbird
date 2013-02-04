package com.twitter.summingbird.storm

import backtype.storm.Config
import com.twitter.bijection.Bijection
import scala.collection.JavaConverters._

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Converts a scala map to and from a storm Config instance.
 */

object ConfigBijection extends Bijection[Map[String,AnyRef],Config] {
  override def apply(config: Map[String,AnyRef]) = {
    val stormConf = new Config
    config foreach { case (k,v) => stormConf.put(k,v) }
    stormConf
  }
  override def invert(config: Config) = config.asScala.toMap
}
