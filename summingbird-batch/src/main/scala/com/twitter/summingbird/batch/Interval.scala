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

package com.twitter.summingbird.batch

// TODO this is clearly more general than summingbird, and should be extended to be a ring (add union, etc...)

/** Represents a single interval on a T with an Ordering
 */
sealed trait Interval[-T] extends (T => Boolean) with java.io.Serializable {
  def contains(t: T): Boolean

  def intersect[U <: T](that: Interval[U]): Interval[U]
  def apply(t: T) = contains(t)
  def &&[U <: T](that: Interval[U]) = intersect(that)
}

case object Universe extends Interval[Any] {
  def contains(t: Any): Boolean = true
  def intersect[U <: Any](that: Interval[U]): Interval[U] = that
}

case object Empty extends Interval[Any] {
  def contains(t: Any): Boolean = false
  def intersect[U <: Any](that: Interval[U]): Interval[U] = this
}

object Interval extends java.io.Serializable {
  def leftClosedRightOpen[T:Ordering](lower: T, upper: T): Interval[T] =
    InclusiveLower(lower) && ExclusiveUpper(upper)
}

// Marker traits to keep lower on the left in Intersection
trait Lower[-T] extends Interval[T]
trait Upper[-T] extends Interval[T]

// TODO ExclusiveLower, InclusiveUpper and unit tests
case class InclusiveLower[T](lower: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Lower[T] {
  def contains(t: T): Boolean = ordering.lteq(lower, t)
  def intersect[U <: T](that: Interval[U]): Interval[U] = that match {
    case Universe => this
    case Empty => that
    case ub@ExclusiveUpper(upper) => if (ub.ordering.lteq(upper, lower)) Empty else Intersection[U](this, ub)
    case lb@InclusiveLower(thatlb) => if (lb.ordering.gt(lower, thatlb)) this else that
    case Intersection(thatL, thatU) => (this && thatL) && thatU
  }
}
case class ExclusiveUpper[T](upper: T)(implicit val ordering: Ordering[T]) extends Interval[T] with Upper[T] {
  def contains(t: T): Boolean = ordering.lt(t, upper)
  def intersect[U <: T](that: Interval[U]): Interval[U] = that match {
    case Universe => this
    case Empty => that
    case lb@InclusiveLower(lower) => if (lb.ordering.lteq(upper, lower)) Empty else Intersection[U](lb, this)
    case ub@ExclusiveUpper(thatub) => if (ub.ordering.lt(upper, thatub)) this else that
    case Intersection(thatL, thatU) => thatL && (this && thatU)
  }
}

case class Intersection[T](lower: Lower[T], upper: Upper[T]) extends Interval[T] {
  def contains(t: T): Boolean = lower.contains(t) && upper.contains(t)
  def intersect[U <: T](that: Interval[U]): Interval[U] = that match {
    case Intersection(thatL, thatU) => (lower && thatL) && (upper && thatU)
    case _ => (lower && that) && (upper && that)
  }
}
