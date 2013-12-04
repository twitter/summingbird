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

package com.twitter.summingbird.online.executor

import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}

class AtomicStateTransformer[T](initState: T) {
  private val curState = new AtomicReference(initState)
  def cur: T = curState.get


  @annotation.tailrec
  final def updateWithState[S](oper: T => (S, T)): (S, T) = {
    val oldState = curState.get
    val (s, newState) = oper(oldState)
    if(curState.compareAndSet(oldState, newState)) {
      (s, newState)
    } else {
      updateWithState(oper)
    }
  }

  final def update(oper: T => T): T =
    updateWithState({x: T => (Unit, oper(x))})._2

}

object InflightTuples {
  private val data = new AtomicInteger(0)
  def incr {
    data.incrementAndGet
  }
  def decr {
    data.decrementAndGet
  }
  def query = {
    data.get
  }

  // WARNING, only use this in testing!!
  def reset {
    data.set(0)
  }
}

case class InputState[T](state: T) {
  InflightTuples.incr

  case class State(counter: Int, failed: Boolean) {
    def fail = this.copy(failed = true, counter = counter - 1)
    def decrBy(by: Int) = this.copy(counter = counter - by )
    def incrBy(by: Int) = this.copy(counter = counter + by )
    def incr = incrBy(1)
    def decr = decrBy(1)
    def doAck = (counter == 0 && !failed)
    def invalidState = counter < 0
  }

  val stateTracking = new AtomicStateTransformer(State(1, false))

  def fanOut(by: Int) = {
    if(by < 0) {
      throw new Exception("Invalid fanout, by should be >= 0")
    }
    val newS = stateTracking.update(_.incrBy(by))
    // If we incremented on something that was 0 or negative
    // And not in a failed state, then this is an error
    if((newS.counter - by <= 0) && !newS.failed) {
      throw new Exception("Invalid call on an inputstate, we had already decremented to 0 and not failed.")
    }
    this
  }

  // Returns true if it should be acked
  def ack(fn: (T => Unit)): Boolean = {
    val newState = stateTracking.update(_.decr)
    if(newState.doAck) {
      InflightTuples.decr
      fn(state)
      true
    } else {
      if(newState.invalidState) throw new Exception("Invalid ack number, logic has failed")
      false
    }
  }

  def fail(fn: (T => Unit)) {
    val newState = stateTracking.update(_.fail)
    InflightTuples.decr
    fn(state)
  }

  override def toString: String = {
    val curState = stateTracking.cur
    "Input State Wrapper(count: %d, failed: %s)".format(curState.counter, curState.failed.toString)
  }
}
