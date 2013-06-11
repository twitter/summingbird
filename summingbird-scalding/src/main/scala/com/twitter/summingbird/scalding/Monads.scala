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
sealed trait StateWithFailure[S,F,+T] {
  def join[U](that: StateWithFailure[S,F,U], mergeErr: (F,F) => F, mergeState: (S,S) => S):
  // TODO: deep joins could blow the stack, not yet using trampoline here
  StateWithFailure[S,F,(T,U)] = StateFn( { (requested: S) =>
      (run(requested), that.run(requested)) match {
        case (Right((s1, r1)), Right((s2, r2))) => Right((mergeState(s1, s2), (r1, r2)))
        case (Left(err1), Left(err2)) => Left(mergeErr(err1, err2)) // Our earlier is not ready
        case (Left(err), _) => Left(err)
        case (_, Left(err)) => Left(err)
      }
    })

  def apply(state: S): Either[F, (S, T)] = run(state)
  def run(state: S): Either[F, (S, T)] = {
    @annotation.tailrec
    def loop(st: StateWithFailure[S, F, Any], stack: List[Any =>  StateWithFailure[S, F, Any]]): Any = {
      st match {
        case StateFn(fn) => stack match {
          case head :: tailStack => loop(head(fn(state)), tailStack)
          case Nil => fn(state) // recursion ends
        }
        case FlatMappedState(st, next) => loop(st, next :: stack)
      }
    }
    loop(this, Nil).asInstanceOf[Either[F, (S, T)]]
  }

  def flatMap[U](next: T => StateWithFailure[S,F,U]): StateWithFailure[S,F,U] =
    FlatMappedState(this, next)

  def map[U](fn: (T) => U): StateWithFailure[S,F,U] =
    FlatMappedState(this, { (t: T) => StateWithFailure.const(fn(t)) })
}

final case class StateFn[S,F,T](fn: S => Either[F, (S, T)]) extends StateWithFailure[S,F,T]
final case class FlatMappedState[S,F,T,U](start: StateWithFailure[S,F,T], fn: T => StateWithFailure[S, F, U]) extends StateWithFailure[S,F,U]

object StateWithFailure {
  def getState[S,F]: StateWithFailure[S,F,S] = StateFn({ (state: S) => Right(state, state) })
  def putState[S,F](newState: S): StateWithFailure[S,F,Unit] = StateFn({ (_: S) => Right(newState, ()) })

  def const[S,F,T](t: T): StateWithFailure[S,F,T] = StateFn({ (state: S) => Right(state, t) })
  def lazyVal[S,F,T](t: => T): StateWithFailure[S,F,T] = StateFn({ (state: S) => Right(state, t) })
  def failure[S,F](f: F): StateWithFailure[S,F,Nothing] = StateFn({ (state: S) => Left(f) })

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
      StateFn({ (s: S) => fn(i).right.map { (s, _) } })
    }
  }
  // TODO this should move to Monad and work for any Monad
  def toKleisli[S] = new FunctionLifter[S]

  implicit def apply[S, F, T](fn: S => Either[F, (S, T)]): StateWithFailure[S,F,T] = StateFn(fn)
  implicit def monad[S,F]: Monad[({type Result[T] = StateWithFailure[S, F, T]})#Result] =
    new StateFMonad[F,S]

  class StateFMonad[F,S] extends Monad[({type Result[T] = StateWithFailure[S, F, T]})#Result] {
    def apply[T](const: T) = { (s: S) => Right((s, const)) }
    def flatMap[T,U](earlier: StateWithFailure[S,F,T])(next: T => StateWithFailure[S,F,U]) = earlier.flatMap(next)
  }
}

// A simple trampoline implementation which we copied for the State monad
sealed trait Trampoline[+A] {
  def map[B](fn: A => B): Trampoline[B]
  def flatMap[B](fn: A => Trampoline[B]): Trampoline[B]
  /**
   * get triggers the computation which is run exactly once
   */
  def get: A
}

final case class Done[A](override val get: A) extends Trampoline[A] {
  def map[B](fn: A => B) = Done(fn(get))
  def flatMap[B](fn: A => Trampoline[B]) = FlatMapped(this, fn)
}

final case class FlatMapped[C, A](start: Trampoline[C], fn: C => Trampoline[A]) extends Trampoline[A] {
  private val finished = new java.util.concurrent.atomic.AtomicReference[AnyRef](null)
  def map[B](fn: A => B) = FlatMapped(this, { (a: A) => Done(fn(a)) })
  def flatMap[B](fn: A => Trampoline[B]) = FlatMapped(this, fn)
  def get = {
    val res = finished.get
    (if(res == null) {
      val computed = Trampoline.run(this).asInstanceOf[AnyRef]
      if (finished.compareAndSet(null, computed)) computed else finished.get
    }
    else res).asInstanceOf[A]
  }
}

object Trampoline {
  val unit: Trampoline[Unit] = Done(())
  def apply[A](a: A): Trampoline[A] = Done(a)
  def lazyVal[A](a: => A): Trampoline[A] = FlatMapped(unit, { (u:Unit) => Done(a) })
  /**
   * Use this to call to another trampoline returning function
   * you break the effect of this if you directly recursively call a Trampoline
   * returning function
   */
  def call[A](layzee: => Trampoline[A]): Trampoline[A] = FlatMapped(unit, { (u:Unit) => layzee })
  implicit val Monad: Monad[Trampoline] = new Monad[Trampoline] {
    def apply[A](a: A) = Done(a)
    def flatMap[A, B](start: Trampoline[A])(fn: A => Trampoline[B]) = start.flatMap(fn)
  }
  // This triggers evaluation. Will reevaluate every time. Prefer .get
  def run[A](tramp: Trampoline[A]): A = {
    @annotation.tailrec
    def loop(start: Trampoline[Any], stack: List[(Any) => Trampoline[Any]]): Any = {
      start match {
        case Done(a) => stack match {
          case next :: tail => loop(next(a), tail)
          case Nil => a
        }
        case FlatMapped(item, fn) => loop(item, fn :: stack)
      }
    }
    // Sorry for the cast, but it is tough to get the types right without a lot of wrapping
    loop(tramp, Nil).asInstanceOf[A]
  }
}
