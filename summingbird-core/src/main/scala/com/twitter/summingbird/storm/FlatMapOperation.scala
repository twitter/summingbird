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

package com.twitter.summingbird.storm

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.FlatMapper
import com.twitter.util.Future
import java.io.{ Closeable, Serializable }

// Represents the logic in the flatMap bolts
trait FlatMapOperation[-T, +U] extends Serializable with Closeable { self =>
  def apply(t: T): Future[TraversableOnce[U]]

  override def close { }

  def andThen[V](fmo: FlatMapOperation[U, V]): FlatMapOperation[T, V] =
    new FlatMapOperation[T, V] {
      def apply(t: T) = self(t).flatMap { tr =>
        val next: Seq[Future[TraversableOnce[V]]] = tr.map { fmo.apply(_) }.toSeq
        Future.collect(next).map(_.flatten) // flatten the inner
      }
      override def close { self.close; fmo.close }
    }
}

object FlatMapOperation {
  def apply[T, U](fmSupplier: => FlatMapper[T, U]): FlatMapOperation[T, U] =
    new FlatMapOperation[T, U] {
      lazy val fm = fmSupplier
      def apply(t: T) = Future.value(fm.encode(t))
    }

  def combine[T, K, V, JoinedV](fmSupplier: => FlatMapOperation[T, (K, V)],
    storeSupplier: () => ReadableStore[K, JoinedV]): FlatMapOperation[T, (K, (V, Option[JoinedV]))] =
    new FlatMapOperation[T, (K, (V, Option[JoinedV]))] {
      lazy val fm = fmSupplier
      lazy val store = storeSupplier()
      override def apply(t: T) =
        fm.apply(t).flatMap { trav: TraversableOnce[(K, V)] =>
          val resultList = trav.toSeq // Can't go through this twice
          val keySet: Set[K] = resultList.map { _._1 }.toSet
          // Do the lookup
          val mres: Map[K, Future[Option[JoinedV]]] = store.multiGet(keySet)
          Future.collect {
            resultList.map { case (k, v) => mres(k).map { k -> (v, _) } }
          }.map { _.toMap }
        }

      override def close {
        fm.close
        store.close
      }
    }
}
