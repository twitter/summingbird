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

package com.twitter.summingbird.storm

import backtype.storm.tuple.Tuple
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

abstract class InputState[T] { self =>
  private val count = new AtomicInteger(0)
  private val failed = new AtomicBoolean(false)

  def expand(by: Int): TupleWrapper = {
    val newVal = count.addAndGet(by)
    self
  }

  // Returns true if it should be acked
  def ack(fn: (T => Unit)): Boolean = {
    if(count.decrementAndGet == 0 && !failed.get) {
      fn(t)
      true
    } else {
      if(count.get < 0) throw new Exception("Invalid ack number, logic has failed")
      false
    }
  }

  def fail(fn: (T => Unit)) {
    failed.set(true)
    fn(t)
  }
  override def toString: String = "Input State Wrapper(count: %d, failed:%s)".format(count.get, failed.get.toString)
}


case class TupleWrapper(t: Tuple) extends InputState[Tuple]