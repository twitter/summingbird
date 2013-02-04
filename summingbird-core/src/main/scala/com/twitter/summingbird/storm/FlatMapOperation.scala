package com.twitter.summingbird.storm

import java.io.{Closeable, Serializable}

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.FlatMapper
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
  def apply[Event, Key, Value](fm: FlatMapper[Event, Key, Value]):
    FlatMapOperation[Event, Key, Value] = new FlatMapOperation[Event, Key, Value] {
      def apply(e: Event) = Future.value(fm.encode(e))
      override def close { fm.cleanup }
    }

  def combine[Event,Key,Value,Joined](fm: FlatMapOperation[Event, Key, Value],
    store: ReadableStore[Key, Joined]): FlatMapOperation[Event, Key, (Value, Option[Joined])] =
    new FlatMapOperation[Event, Key, (Value, Option[Joined])] {
      override def apply(e: Event) =
        fm.apply(e).flatMap { trav: TraversableOnce[(Key, Value)] =>
          val resultList = trav.toList // Can't go through this twice
          val keySet: Set[Key] = resultList.map { _._1 }.toSet
          // Do the lookup
          store.multiGet(keySet).map { mres: Map[Key, Joined] =>
            resultList.map { case (k,v) => (k, (v, mres.get(k))) }
          }
        }

      override def close {
        fm.close
        store.close
      }
    }
}
