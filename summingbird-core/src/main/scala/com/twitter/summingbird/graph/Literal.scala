/*
 Copyright 2014 Twitter, Inc.

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

package com.twitter.summingbird.graph

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[T, N[_]] {
  def evaluate: N[T] = Literal.evaluate(this)
}
case class ConstLit[T, N[_]](override val evaluate: N[T]) extends Literal[T, N]
case class UnaryLit[T1, T2, N[_]](arg: Literal[T1, N],
    fn: N[T1] => N[T2]) extends Literal[T2, N] {
}
case class BinaryLit[T1, T2, T3, N[_]](arg1: Literal[T1, N], arg2: Literal[T2, N],
    fn: (N[T1], N[T2]) => N[T3]) extends Literal[T3, N] {
}

object Literal {
  /**
   * This evaluates a literal formula back to what it represents
   * being careful to handle diamonds by creating referentially
   * equivalent structures (not just structurally equivalent)
   */
  def evaluate[T, N[_]](lit: Literal[T, N]): N[T] =
    evaluate(HMap.empty[({ type L[T] = Literal[T, N] })#L, N], lit)._2

  // Memoized version of the above to handle diamonds
  protected def evaluate[T, N[_]](hm: HMap[({ type L[T] = Literal[T, N] })#L, N], lit: Literal[T, N]): (HMap[({ type L[T] = Literal[T, N] })#L, N], N[T]) =
    hm.get(lit) match {
      case Some(prod) => (hm, prod)
      case None =>
        lit match {
          case ConstLit(prod) => (hm + (lit -> prod), prod)
          case UnaryLit(in, fn) =>
            val (h1, p1) = evaluate(hm, in)
            val p2 = fn(p1)
            (h1 + (lit -> p2), p2)
          case BinaryLit(in1, in2, fn) =>
            val (h1, p1) = evaluate(hm, in1)
            val (h2, p2) = evaluate(h1, in2)
            val p3 = fn(p1, p2)
            (h2 + (lit -> p3), p3)
        }
    }
}
