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

package com.twitter.summingbird.monad

import com.twitter.algebird.Monad
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._

object MonadLaws extends Properties("Monad") {

  // Mutually recursive functions
  def ping(todo: Int, acc: Int): Trampoline[Int] =
    if(todo <= 0) Trampoline(acc) else Trampoline.call(pong(todo - 1, acc + 1))

  def pong(todo: Int, acc: Int): Trampoline[Int] =
    if(todo <= 0) Trampoline(acc) else Trampoline.call(ping(todo - 1, acc + 1))

  property("Trampoline should run without stackoverflow") =
    forAll { (b: Int) =>
      val bsmall = b % 1000000
      ping(bsmall, 0).get == (bsmall max 0)
    }

  property("get/swap") = forAll { (i: Int) =>
    val fn = for {
      start <- StateWithError.getState[Int, String]
      oldState <- StateWithError.swapState[Int, String](start * 2)
    } yield oldState

    fn(i) == Right((2 * i, i))
  }

  property("State behaves correctly") = forAll { (in: Int, head: Long, fns: List[(Int) => Either[String, (Int, Long)]]) =>
    val mons = fns.map { StateWithError(_) }
    val comp = mons.foldLeft(StateWithError.const[Int, String, Long](head)) { (old, fn) =>
      old.flatMap( x => fn) // just bind
    }
    comp(in) == (fns.foldLeft(Right((in, head)): Either[String, (Int, Long)]) { (oldState, fn) =>
      oldState.right.flatMap { case (s, v) => fn(s) }
    })
  }
}
