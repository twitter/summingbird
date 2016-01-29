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

package com.twitter.summingbird.online

import com.twitter.storehaus.ReadableStore
import com.twitter.util.{ Future, Await }
import java.io.{ Closeable, Serializable }

// Represents the logic in the flatMap bolts
trait FlatMapOperation[-T, +U] extends Serializable with Closeable {
  def apply(t: T): Future[TraversableOnce[U]]

  override def close {}

  /*
   * maybeFlush may be periodically called to empty any internal state
   * Not used yet so commented out
   */
  def maybeFlush: Future[TraversableOnce[U]] = Future.value(Seq.empty[U])

  // Helper to add a simple U => S operation at the end of a FlatMapOperation
  def map[S](fn: U => S): FlatMapOperation[T, S] =
    andThen(FlatMapOperation(fn.andThen(r => Iterator(r))))

  // Helper to add a simple U => TraversableOnce[S] operation at the end of a FlatMapOperation
  def flatMap[S](fn: U => TraversableOnce[S]): FlatMapOperation[T, S] =
    andThen(FlatMapOperation(fn))

  /**
   * TODO: Think about getting an implicit FutureCollector here, in
   * case we don't want to completely choke on large expansions (and
   * joins).
   */
  def andThen[V](fmo: FlatMapOperation[U, V]): FlatMapOperation[T, V] = {
    val self = this // Using the standard "self" at the top of the
    // trait caused a nullpointerexception after
    // serialization. I think that Kryo mis-serializes that reference.
    new FlatMapOperation[T, V] {
      def apply(t: T) = self(t).flatMap { tr =>
        val next: Seq[Future[TraversableOnce[V]]] = tr.map { fmo.apply(_) }.toIndexedSeq
        Future.collect(next).map(_.flatten) // flatten the inner
      }

      override def maybeFlush = {
        self.maybeFlush.flatMap { x: TraversableOnce[U] =>
          val z: IndexedSeq[Future[TraversableOnce[V]]] = x.map(fmo.apply(_)).toIndexedSeq
          val w: Future[Seq[V]] = Future.collect(z).map(_.flatten)
          for {
            ws <- w
            maybes <- fmo.maybeFlush
            maybeSeq = maybes.toSeq
          } yield ws ++ maybeSeq
        }
      }
      override def close { self.close; fmo.close }
    }
  }
}

class FunctionFlatMapOperation[T, U](@transient fm: T => TraversableOnce[U])
    extends FlatMapOperation[T, U] {
  val boxed = Externalizer(fm)
  def apply(t: T) = Future.value(boxed.get(t))
}

class GenericFlatMapOperation[T, U](@transient fm: T => Future[TraversableOnce[U]])
    extends FlatMapOperation[T, U] {
  val boxed = Externalizer(fm)
  def apply(t: T) = boxed.get(t)
}

class FunctionKeyFlatMapOperation[K1, K2, V](@transient fm: K1 => TraversableOnce[K2])
    extends FlatMapOperation[(K1, V), (K2, V)] {
  val boxed = Externalizer(fm)
  def apply(t: (K1, V)) = {
    Future.value(boxed.get(t._1).map { newK => (newK, t._2) })
  }
}

class IdentityFlatMapOperation[T] extends FlatMapOperation[T, T] {
  // By default we do the identity function
  def apply(t: T): Future[TraversableOnce[T]] = Future.value(Some(t))

  // But if we are composed with something else, just become it
  override def andThen[V](fmo: FlatMapOperation[T, V]): FlatMapOperation[T, V] = fmo
}

object FlatMapOperation {
  def identity[T]: FlatMapOperation[T, T] = new IdentityFlatMapOperation()

  def apply[T, U](fm: T => TraversableOnce[U]): FlatMapOperation[T, U] =
    new FunctionFlatMapOperation(fm)

  def generic[T, U](fm: T => Future[TraversableOnce[U]]): FlatMapOperation[T, U] =
    new GenericFlatMapOperation(fm)

  def keyFlatMap[K1, K2, V](fm: K1 => TraversableOnce[K2]): FlatMapOperation[(K1, V), (K2, V)] =
    new FunctionKeyFlatMapOperation(fm)

  def combine[T, K, V, JoinedV](fmSupplier: => FlatMapOperation[T, (K, V)],
    storeSupplier: OnlineServiceFactory[K, JoinedV]): FlatMapOperation[T, (K, (V, Option[JoinedV]))] =
    new FlatMapOperation[T, (K, (V, Option[JoinedV]))] {
      lazy val fm = fmSupplier
      lazy val store = storeSupplier.serviceStore()
      override def apply(t: T) =
        fm.apply(t).flatMap { trav: TraversableOnce[(K, V)] =>
          val resultList = trav.toSeq // Can't go through this twice
          val keySet: Set[K] = resultList.map { _._1 }.toSet

          if (keySet.isEmpty)
            Future.value(Map.empty)
          else {
            // Do the lookup
            val mres: Map[K, Future[Option[JoinedV]]] = store.multiGet(keySet)
            val resultFutures = resultList.map { case (k, v) => mres(k).map { k -> (v, _) } }.toIndexedSeq
            Future.collect(resultFutures)
          }
        }

      override def close {
        fm.close
        Await.result(store.close)
      }
    }

  def write[T](sinkSupplier: () => (T => Future[Unit])) =
    new WriteOperation[T](sinkSupplier)
}

class WriteOperation[T](sinkSupplier: () => (T => Future[Unit])) extends FlatMapOperation[T, T] {
  lazy val sink = sinkSupplier()
  override def apply(t: T) = sink(t).map { _ => Some(t) }
}
