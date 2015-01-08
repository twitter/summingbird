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

package com.twitter.summingbird.planner

/**
 * This is a marker trait that indicates that
 * the subclass holds one or more irreducible
 * items passed by the user. These may need to
 * be accessed at some planning stage after
 * optimization in order to attach the correct
 * options
 */
trait IrreducibleContainer {
  def irreducibles: Iterable[Any]
}

object IrreducibleContainer {
  def flatten(item: Any): Iterable[Any] = item match {
    case (ic: IrreducibleContainer) => ic.irreducibles
    case _ => Seq(item)
  }

  def flatten(left: Any, right: Any): Iterable[Any] = (left, right) match {
    case (li: IrreducibleContainer, ri: IrreducibleContainer) => li.irreducibles ++ ri.irreducibles
    case (li: IrreducibleContainer, _) => Seq(right) ++ li.irreducibles
    case (_, ri: IrreducibleContainer) => Seq(left) ++ ri.irreducibles
    case _ => Seq(left, right)
  }
}

/**
 * When optimizing and composing flatMaps, this class can be used
 * so that we can recover the parts if needed. For instance,
 * options apply to the irreducible inputs from the user (such as
 * functions, stores, etc... This class allows us to get those
 * irreducibiles even after optimization
 */
case class ComposedFlatMap[A, B, C](first: A => TraversableOnce[B],
    second: B => TraversableOnce[C]) extends (A => TraversableOnce[C]) with IrreducibleContainer {

  // Note we don't allocate a new Function in the apply call,
  // we reuse the instances above.
  def apply(a: A) = first(a).flatMap(second)
  def irreducibles = IrreducibleContainer.flatten(first, second)
}

/**
 * Composing optionMaps
 */
case class ComposedOptionMap[A, B, C](first: A => Option[B],
    second: B => Option[C]) extends (A => Option[C]) with IrreducibleContainer {

  // Note we don't allocate a new Function in the apply call,
  // we reuse the instances above.
  def apply(a: A) = first(a).flatMap(second)
  def irreducibles = IrreducibleContainer.flatten(first, second)
}

case class ComposedOptionFlat[A, B, C](first: A => Option[B],
    second: B => TraversableOnce[C]) extends (A => TraversableOnce[C]) with IrreducibleContainer {

  // Note we don't allocate a new Function in the apply call,
  // we reuse the instances above.
  def apply(a: A) = first(a).map(second).getOrElse(Iterator.empty)
  def irreducibles = IrreducibleContainer.flatten(first, second)
}

case class OptionToFlat[A, B](optionMap: A => Option[B])
    extends (A => TraversableOnce[B]) with IrreducibleContainer {
  // Note we don't allocate a new Function in the apply call,
  // we reuse the instances above.
  def apply(a: A) = optionMap(a).toIterator
  def irreducibles = IrreducibleContainer.flatten(optionMap)
}

/*
 * FlatMaps are often actually filters. If evaluating the function for is cheap, you can
 * create a savings by checking to see if A will have any results. If it has None, don't
 * send it down stream.
 *
 * This may be useful in Storm where we want to filter before serializing out of
 * the spouts (or other similar fixed-source parition systems)
 */
case class FlatAsFilter[A](useAsFilter: A => TraversableOnce[Nothing]) extends (A => Option[A]) with IrreducibleContainer {
  def apply(a: A) = if (useAsFilter(a).isEmpty) None else Some(a)
  def irreducibles = IrreducibleContainer.flatten(useAsFilter)
}

/**
 * (a.flatMap(f1) ++ a.flatMap(f2)) == a.flatMap { i => f1(i) ++ f2(i) }
 */
case class MergeResults[A, B](left: A => TraversableOnce[B], right: A => TraversableOnce[B])
    extends (A => TraversableOnce[B]) with IrreducibleContainer {
  // TODO it is not totally clear the fastest way to merge two TraversableOnce instances
  // If they are iterators or iterables, this should be fast
  def apply(a: A) = (left(a).toIterator) ++ (right(a).toIterator)
  def irreducibles = IrreducibleContainer.flatten(left, right)
}
