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
import com.twitter.algebird.Semigroup

object IteratorSums extends java.io.Serializable {

  def groupedSum[K1,V1:Semigroup](in: Iterator[(K1, V1)]): Iterator[(K1, V1)] =
    new Iterator[(K1, V1)] {
      var nextPair: Option[(K1,V1)] = None
      def hasNext = nextPair.isDefined || in.hasNext

      def advanced: Unit = {
        if(nextPair.isEmpty && in.hasNext) nextPair = Some(in.next)
      }
      def takeNext: (K1, V1) = {
        advanced
        val res = nextPair.get
        nextPair = None
        res
      }

      def next = {
        @annotation.tailrec
        def sum(acc: (K1,V1)): (K1, V1) = {
          advanced;
          (acc, nextPair) match {
            case ((ka, va), Some((k, v))) if (ka == k) =>
              nextPair = None // consume
              sum((k, Semigroup.plus(va, v)))
            case _ => acc
          }
        }
        sum(takeNext)
      }
    }

  /**
   * This assumes the U are safe to ignore and are just passed through. It
   * goes through the values and gives you the sumOption just before
   * this value and the current value
   */
  def partials[U,V:Semigroup](in: Iterator[(U, V)]): Iterator[(U, (Option[V], V))] = {
    var prev: Option[V] = None
    in.map { case (k, v) =>
      val stored = prev
      prev = Some(prev.map(Semigroup.plus(_, v)).getOrElse(v))
      (k, (stored, v))
    }
  }
}
