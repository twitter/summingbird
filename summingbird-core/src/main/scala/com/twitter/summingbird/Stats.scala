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

package com.twitter.summingbird

import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import java.util.UUID

object Stats {
    @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
    private val lookupProvider = new ConcurrentHashMap[String, Incrementor]
    def addIncrementor(id:String, inc: Incrementor) = lookupProvider.put(id, inc)
    def doLookup(id: String):Option[Incrementor] = Option(lookupProvider.get(id))

    val nullIncrementor = new Incrementor {
        def incrBy(amount: Long) {}
    }
}

trait Incrementor {
  def incrBy(amount: Long)
}

case class Stats(name: String){
  val uniqueId = "%s |::::| %s".format(name, UUID.randomUUID.toString)
  lazy val incrementor: Incrementor = Stats.doLookup(uniqueId).getOrElse(Stats.nullIncrementor)

  def incr = incrBy(1L)
  def incrBy(amount: Long) = incrementor.incrBy(amount)
}
