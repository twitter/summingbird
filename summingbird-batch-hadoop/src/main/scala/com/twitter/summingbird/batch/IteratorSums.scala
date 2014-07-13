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
import com.twitter.algebird.{ Semigroup, StatefulSummer }

import scala.collection.mutable.ArrayBuffer

private[summingbird] object IteratorSums extends java.io.Serializable {

  def sumWith[T](it: Iterator[T], summer: StatefulSummer[T]): Iterator[T] =
    // this is for MAXIMUM speed. Any ideas to speed it up, say so + benchmark
    it.map(summer.put(_)).filter(_.isDefined).map(_.get) ++ summer.flush.iterator

  // get a big block, but not so big to OOM
  def optimizedPairSemigroup[T1: Semigroup, T2: Semigroup](blockSize: Int): Semigroup[(T1, T2)] =
    new Semigroup[(T1, T2)] {
      def plus(a: (T1, T2), b: (T1, T2)) = {
        (Semigroup.plus(a._1, b._1), Semigroup.plus(a._2, b._2))
      }
      override def sumOption(items: TraversableOnce[(T1, T2)]): Option[(T1, T2)] = {
        if (items.isEmpty) None
        else {
          val op = new BufferOp[(T1, T2)](blockSize) {
            def operate(items: Seq[(T1, T2)]): Option[(T1, T2)] = for {
              t1 <- Semigroup.sumOption(items.iterator.map(_._1))
              t2 <- Semigroup.sumOption(items.iterator.map(_._2))
            } yield (t1, t2)
          }
          items.foreach(op.put(_))
          op.flush
        }
      }
    }

  abstract class BufferOp[V](sz: Int) extends java.io.Serializable {
    def operate(items: Seq[V]): Option[V]

    require(sz > 0, "buffer <= 0 not allowed")
    val buffer = new ArrayBuffer[V](sz)

    def put(v: V): Option[V] = {
      buffer += v
      if (buffer.size > sz) flush.flatMap(put(_)) // put it back in the front
      None
    }

    def isFlushed = buffer.isEmpty

    def flush: Option[V] = {
      val res = operate(buffer)
      buffer.clear
      res
    }
  }

  def bufferStatefulSummer[V: Semigroup](sz: Int): StatefulSummer[V] =
    new BufferOp[V](sz) with StatefulSummer[V] {
      def semigroup = implicitly[Semigroup[V]]
      def operate(items: Seq[V]) = Semigroup.sumOption(items)
    }

  def groupedStatefulSummer[K: Equiv, V: Semigroup](sz: Int): StatefulSummer[(K, V)] = new StatefulSummer[(K, V)] {
    require(sz > 0, "buffer <= 0 not allowed")

    // The StatefulSummer (wrongly?) needs this, but it is never used
    def semigroup = Semigroup.from {
      case ((lk, lv), (rk, rv)) =>
        // if the keys match, sum, else return the new pair
        if (Equiv[K].equiv(lk, rk)) (rk, Semigroup.plus(lv, rv))
        else (rk, rv)
    }

    var lastK: Option[K] = None
    val buffer = new ArrayBuffer[V](sz)

    def put(kv: (K, V)): Option[(K, V)] = {
      val (k, v) = kv
      val res = lastK.flatMap { lk =>
        if (!Equiv[K].equiv(lk, k)) flush
        else if (buffer.size > sz) flush.flatMap(put(_)) // put it back in the front
        else None
      }
      lastK = Some(k)
      buffer += v
      res
    }

    def isFlushed = buffer.isEmpty

    def flush = {
      // sumOption is highly optimized
      val res = Semigroup.sumOption(buffer).flatMap { sv => lastK.map((_, sv)) }
      buffer.clear
      res
    }
  }

  def groupedSum[K1: Equiv, V1: Semigroup](in: Iterator[(K1, V1)], bufferSize: Int = 1000): Iterator[(K1, V1)] =
    sumWith(in, groupedStatefulSummer(bufferSize))

  /**
   * This assumes the U are safe to ignore and are just passed through. It
   * goes through the values and gives you the sumOption just before
   * this value and the current value
   */
  def partials[U, V: Semigroup](in: Iterator[(U, V)]): Iterator[(U, (Option[V], V))] = {
    var prev: Option[V] = None
    in.map {
      case (k, v) =>
        val stored = prev
        prev = Some(prev.map(Semigroup.plus(_, v)).getOrElse(v))
        (k, (stored, v))
    }
  }
}
