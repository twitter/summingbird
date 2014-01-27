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

package com.twitter.summingbird.memory

import com.twitter.summingbird._
import collection.mutable.{ Map => MMap }

object MemoryStats {
  val stats = MMap[String, Long]()


  def get(stat: Stats) = stats.synchronized {
      stats.getOrElse(stat.uniqueId, 0L)
  }

  def printStats = stats.synchronized {
    println("Printing stats...")
    stats.foreach(tup => println("%s => %d".format(tup._1, tup._2)))
  }

  def getStats: Map[String, Long] = stats.synchronized {
    stats.toMap
  }

  def getIncrementor(uniqueId: String) = new Incrementor {
    def incrBy(amount: Long) {
      stats.synchronized {
        val oldVal = stats.getOrElse(uniqueId, 0L)
        stats.put(uniqueId, oldVal + amount)
      }
    }
  }
}
