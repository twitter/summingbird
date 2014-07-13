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

package com.twitter.summingbird.batch

import com.twitter.bijection.Bijection
import java.util.{ Map => JMap }
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

private[summingbird] object ConfigBijection {
  implicit val fromMap: Bijection[Map[String, AnyRef], Configuration] =
    new Bijection[Map[String, AnyRef], Configuration] {
      override def apply(config: Map[String, AnyRef]) = {
        val conf = new Configuration(false) // false means don't read defaults
        config foreach { case (k, v) => conf.set(k, v.toString) }
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
  val fromJavaMap: Bijection[JMap[String, AnyRef], Configuration] =
    Bijection.connect[JMap[String, AnyRef], Map[String, AnyRef], Configuration]
}
