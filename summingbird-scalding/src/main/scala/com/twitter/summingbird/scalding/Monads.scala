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

package com.twitter.summingbird.scalding

import com.twitter.algebird.Monad

// TODO this is general, move somewhere better

// Reader Monad, represents a series of operations that mutate some environment
// type (the input to the function)

class Reader[Env, +T](val fn: Env => T) {
  def apply(e: Env): T = fn(e)
  def flatMap[U](next: T => Reader[Env, U]): Reader[Env, U] =
    Reader((e: Env) => next(fn(e)).fn(e))
  def map[U](thatFn: T => U): Reader[Env, U] =
    Reader((e: Env) => thatFn(fn(e)))
}

object Reader {
  implicit def apply[E,T](fn: (E) => T): Reader[E, T] = new Reader[E, T](fn)

  class ReaderM[Env] extends Monad[({type Result[T] = Reader[Env,T]})#Result] {
    def apply[T](t: T) = Reader((e: Env) => t)
    def flatMap[T,U](self: Reader[Env, T])(next: T => Reader[Env, U]) = self.flatMap(next)
    override def map[T,U](self: Reader[Env, T])(fn: T => U) = self.map(fn)
  }
  implicit def monad[Env]: Monad[({type Result[T] = Reader[Env,T]})#Result] = new ReaderM[Env]
}

// Monad for either, allows us to use for comprhension with Either
object EitherM {
  class EitherMonad[L] extends Monad[({type RightType[R] = Either[L,R]})#RightType] {
    def apply[R](r: R) = Right(r)

    def flatMap[T,U](self: Either[L,T])(next: T => Either[L, U]): Either[L, U] =
      self.right.flatMap(next)

    override def map[T,U](self: Either[L,T])(fn: T => U): Either[L, U] =
      self.right.map(fn)
  }
  implicit def monad[L]: Monad[({type RightT[R] = Either[L,R]})#RightT] = new EitherMonad[L]
}

// A Monad combinator here could be used, but it would make the code even more unreadable (for now)
class StateWithFailure[S,F,+T](fn: S => Either[F, (S, T)]) {
  def join[U](that: StateWithFailure[S,F,U], mergeErr: (F,F) => F, mergeState: (S,S) => S):
  StateWithFailure[S,F,(T,U)] = { (requested: S) => {
      ((run(requested), that.run(requested)) match {
        case (Right((s1, r1)), Right((s2, r2))) => Right((mergeState(s1, s2), (r1, r2)))
        case (Left(err1), Left(err2)) => Left(mergeErr(err1, err2)) // Our earlier is not ready
        case (Left(err), _) => Left(err)
        case (_, Left(err)) => Left(err)
      }) : Either[F, (S, (T, U))]
    }
  }
  def apply(state: S): Either[F, (S, T)] = fn(state)
  def run(state: S): Either[F, (S, T)] = fn(state)

  def flatMap[U](next: T => StateWithFailure[S,F,U]): StateWithFailure[S,F,U] =
    StateWithFailure( (requested: S) =>
      run(requested).right.flatMap { case (available, result) =>
        next(result).run(available)
      }
    )
  def map[U](fn: (T) => U): StateWithFailure[S,F,U] =
    StateWithFailure( (requested: S) =>
      run(requested).right.map { case (available, result) =>
        (available, fn(result))
      }
    )
}

/**
 * If the graphs get really big we might need to trampoline:
 * http://apocalisp.wordpress.com/2011/10/26/tail-call-elimination-in-scala-monads/
 */
object StateWithFailure {
  def getState[S,F]: StateWithFailure[S,F,S] = { (state: S) => Right(state, state) }
  def putState[S,F](newState: S): StateWithFailure[S,F,Unit] = { (_: S) => Right(newState, ()) }

  class ConstantStateMaker[S] {
    def apply[F, T](either: Either[F, T]): StateWithFailure[S, F, T] =
      { (s: S) => either.right.map { (s, _) } }
  }

  // Trick to avoid having to give all the types
  // fromEither(Left("oh, no"))
  // should work
  def fromEither[S] = new ConstantStateMaker[S]
  class FunctionLifter[S] {
    def apply[I, F, T](fn: I => Either[F, T]): (I => StateWithFailure[S, F, T]) = { (i: I) =>
      StateWithFailure({ (s: S) => fn(i).right.map { (s, _) } })
    }
  }
  // TODO this should move to Monad and work for any Monad
  def toKleisli[S] = new FunctionLifter[S]

  implicit def apply[S, F, T](fn: S => Either[F, (S, T)]): StateWithFailure[S,F,T] = new StateWithFailure(fn)
  implicit def monad[S,F]: Monad[({type Result[T] = StateWithFailure[S, F, T]})#Result] =
    new StateFMonad[F,S]

  class StateFMonad[F,S] extends Monad[({type Result[T] = StateWithFailure[S, F, T]})#Result] {
    def apply[T](const: T) = { (s: S) => Right((s, const)) }
    def flatMap[T,U](earlier: StateWithFailure[S,F,T])(next: T => StateWithFailure[S,F,U]) = earlier.flatMap(next)
    override def map[T,U](earlier: StateWithFailure[S,F,T])(fn: (T) => U) = earlier.map(fn)
  }
}
