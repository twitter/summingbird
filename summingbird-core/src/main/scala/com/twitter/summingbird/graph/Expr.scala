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
 * The variables are assigned Ids. Each Id is associate with
 * an expression of inner type T.
 *
 * T is a phantom type used by the type system
 */
final case class Id[T](id: Int)

/**
 * Here we replace each literal with a variable versions of the above.
 */
sealed trait Expr[T, N[_]] {
  def evaluate(idToExp: HMap[Id, ({ type E[t] = Expr[t, N] })#E]): N[T]
}
case class Const[T, N[_]](value: N[T]) extends Expr[T, N] {
  def evaluate(idToExp: HMap[Id, ({ type E[t] = Expr[t, N] })#E]): N[T] = value
}
case class Var[T, N[_]](name: Id[T]) extends Expr[T, N] {
  def evaluate(idToExp: HMap[Id, ({ type E[t] = Expr[t, N] })#E]): N[T] =
    idToExp(name).evaluate(idToExp)
}
case class Unary[T1, T2, N[_]](arg: Id[T1], fn: N[T1] => N[T2]) extends Expr[T2, N] {
  def evaluate(idToExp: HMap[Id, ({ type E[t] = Expr[t, N] })#E]): N[T2] =
    fn(idToExp(arg).evaluate(idToExp))
}
case class Binary[T1, T2, T3, N[_]](arg1: Id[T1],
    arg2: Id[T2],
    fn: (N[T1], N[T2]) => N[T3]) extends Expr[T3, N] {
  def evaluate(idToExp: HMap[Id, ({ type E[t] = Expr[t, N] })#E]): N[T3] =
    fn(idToExp(arg1).evaluate(idToExp), idToExp(arg2).evaluate(idToExp))
}

