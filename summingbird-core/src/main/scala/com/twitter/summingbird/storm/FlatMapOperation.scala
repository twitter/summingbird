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

import java.io.{Closeable, Serializable}

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.FlatMapper
import com.twitter.summingbird.sink.OnlineSink
import com.twitter.util.Future

// Represents the logic in the flatMap bolts
trait FlatMapOperation[Event,Key,Value] extends Serializable with Closeable { self =>
  def apply(e: Event): Future[TraversableOnce[(Key,Value)]]
  override def close {}
  def andThen[K2,V2](fmo: FlatMapOperation[(Key,Value), K2, V2]): FlatMapOperation[Event,K2,V2] =
    new FlatMapOperation[Event,K2,V2] {
      def apply(e: Event) = self(e).flatMap { tr =>
        val next: Seq[Future[TraversableOnce[(K2,V2)]]] = tr.map { fmo.apply(_) }.toSeq
        Future.collect(next).map { t => t.flatMap { tr => tr } } // flatten the inner
      }
      override def close { self.close; fmo.close }
    }
}

object FlatMapOperation {
  def apply[Event, Key, Value](fmSupplier: => FlatMapper[Event, Key, Value]):
      FlatMapOperation[Event, Key, Value] = new FlatMapOperation[Event, Key, Value] {
    lazy val fm = fmSupplier
    def apply(e: Event) = Future.value(fm.encode(e))
    override def close { fm.cleanup }
  }

  def combine[Event,Key,Value,Joined](fmSupplier: => FlatMapOperation[Event, Key, Value],
    storeSupplier: () => ReadableStore[Key, Joined]): FlatMapOperation[Event, Key, (Value, Option[Joined])] =
    new FlatMapOperation[Event, Key, (Value, Option[Joined])] {
      lazy val fm = fmSupplier
      lazy val store = storeSupplier()
      override def apply(e: Event) =
        fm.apply(e).flatMap { trav: TraversableOnce[(Key, Value)] =>
          val resultList = trav.toSeq // Can't go through this twice
          val keySet: Set[Key] = resultList.map { _._1 }.toSet
          // Do the lookup
          val mres: Map[Key, Future[Option[Joined]]] = store.multiGet(keySet)
          Future.collect {
            resultList.map { case (k, v) => mres(k).map { k -> (v, _) } }
          }.map { _.toMap }
        }

      override def close {
        fm.close
        store.close
      }
    }

  def write[Event, Key, Value, Written](fmSupplier: => FlatMapOperation[Event, Key, Value],
      sinkSupplier: () => OnlineSink[Written], conversion: ((Key, Value)) => TraversableOnce[Written]) =
      new FlatMapOperation[Event, Key, Value] {
        lazy val fm = fmSupplier
        lazy val sink = sinkSupplier()

        override def apply(e: Event) =
          fm.apply(e).flatMap { pairs =>
            val writes = pairs.flatMap { pair => conversion(pair).map { sink.write _ } }
            Future.collect(writes.toList).map { _ => pairs }
          }
      }
}
