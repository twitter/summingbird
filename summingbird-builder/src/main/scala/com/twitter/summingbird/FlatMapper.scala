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

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

abstract class FlatMapper[-T, +U] extends (T => TraversableOnce[U]) {
  // transform an event to key value pairs
  def encode(t: T): TraversableOnce[U]
  def apply(t: T) = encode(t)
}

// FunctionFlatMapper allows the user to provide a flatmapping
// Function1 to the Summingbird DSL directly.

class FunctionFlatMapper[T, U](fn: T => TraversableOnce[U]) extends FlatMapper[T, U] {
  override def encode(t: T) = fn(t)
}

// Implicit conversion to facilitate the use of Function1 described
// above.

object FlatMapper {
  implicit def functionToFlatMapper[T, U](fn: T => TraversableOnce[U]): FlatMapper[T, U] =
    new FunctionFlatMapper(fn)

  def andThen[T, U, V](fm: FlatMapper[T, U], fm2: FlatMapper[U, V]): FlatMapper[T, V] =
    new FlatMapper[T, V] {
      override def encode(t: T) = fm.encode(t).flatMap { fm2.encode(_) }
    }

  def filter[T, U](fm: FlatMapper[T, U], filterfn: U => Boolean): FlatMapper[T, U] =
    new FlatMapper[T, U] {
      override def encode(t: T) = fm.encode(t).filter(filterfn)
    }
}
