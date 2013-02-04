package com.twitter.summingbird.scalding

import com.twitter.bijection.Bijection
import java.util.{ Map => JMap }
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object ConfigBijection {
  implicit val fromMap: Bijection[Map[String, AnyRef], Configuration] =
    new Bijection[Map[String, AnyRef], Configuration] {
      override def apply(config: Map[String, AnyRef]) = {
        val conf = new Configuration
        config foreach { case (k,v) => conf.set(k,v.toString) }
        conf
      }
      override def invert(config: Configuration) =
        config.asScala
          .foldLeft(Map[String, AnyRef]()) { (m, entry) =>
            val k = entry.getKey
            val v = entry.getValue
            m + (k -> v)
          }
    }
  val fromJavaMap: Bijection[JMap[String,AnyRef], Configuration] =
    Bijection.connect[JMap[String,AnyRef], Map[String,AnyRef], Configuration]
}
