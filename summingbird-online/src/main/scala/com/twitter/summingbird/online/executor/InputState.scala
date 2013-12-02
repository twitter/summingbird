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

import java.util.concurrent.atomic.AtomicReference

class AtomicStateTransformer[T](initState: T) {
  private val curState = new AtomicReference(initState)
  def cur: T = curState.get

  @annotation.tailrec
  final def updateState(oper: T => T): T = {
    val oldState = curState.get
    val newState = oper(oldState)
    if(curState.compareAndSet(oldState, newState)) {
      newState
    } else {
      updateState(oper)
    }
  }
}

case class InputState[T](state: T, initValue: Int) {
  case class State(counter: Int, failed: Boolean) {
    def decr = this.copy(counter = counter - 1)
    def fail = this.copy(failed = true, counter = counter - 1)
    def doAck = (counter == 0 && !failed)
    def invalidState = counter < 0
  }

  val stateTracking = new AtomicStateTransformer(State(initValue, false))

  if(initValue == 0) {
    throw new Exception("Invalid initial value, input state must start >= 1")
  }

  // Returns true if it should be acked
  def ack(fn: (T => Unit)): Boolean = {
    val newState = stateTracking.updateState(_.decr)
    if(newState.doAck) {
      fn(state)
      println("Acking, count is:" + newState.counter + ", initial was: " + initValue)
      true
    } else {
      if(newState.invalidState) throw new Exception("Invalid ack number, logic has failed")
      println("Not acking, count is:" + newState.counter + ", initial was: " + initValue)
      false
    }
  }

  def fail(fn: (T => Unit)) {
    val newState = stateTracking.updateState(_.fail)
    fn(state)
  }

  override def toString: String = {
    val curState = stateTracking.cur
    "Input State Wrapper(count: %d, failed: %s)".format(curState.counter, curState.failed.toString)
  }
}
