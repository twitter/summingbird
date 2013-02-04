package com.twitter.summingbird.scalding

import cascading.flow.FlowDef

import com.twitter.scalding.{Mode, TypedPipe}
import com.twitter.summingbird.FlatMapper
import com.twitter.summingbird.service.OfflineService

import java.io.Serializable

// Represents the logic in the flatMap offline
trait FlatMapOperation[Event,Key,Value] extends Serializable { self =>
  def apply[Time](env: ScaldingEnv, timeEv: TypedPipe[(Time, Event)])
    (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time, (Key, Value))]

  def andThen[K2,V2](fmo: FlatMapOperation[(Key,Value), K2, V2]): FlatMapOperation[Event,K2,V2] =
    new FlatMapOperation[Event,K2,V2] {
      def apply[Time](env: ScaldingEnv, timeEv: TypedPipe[(Time, Event)])
        (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time, (K2, V2))] =
        fmo.apply(env, self.apply(env, timeEv))
    }
}

object FlatMapOperation {
  def apply[Event, Key, Value](fm: FlatMapper[Event, Key, Value]):
    FlatMapOperation[Event, Key, Value] = new FlatMapOperation[Event, Key, Value] {
      def apply[Time](env: ScaldingEnv, timeEv: TypedPipe[(Time, Event)])
        (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time, (Key, Value))] = {
        timeEv.flatMap { case (t: Time, e: Event) =>
          // TODO remove toList when scalding supports TraversableOnce
          fm.encode(e).map { (t, _) }.toList
        }
      }
    }

  def combine[Event,Key,Value,Joined](fm: FlatMapOperation[Event, Key, Value],
    service: OfflineService[Key, Joined]): FlatMapOperation[Event, Key, (Value, Option[Joined])] =

    new FlatMapOperation[Event, Key, (Value, Option[Joined])] {
      def apply[Time](env: ScaldingEnv, timeEv: TypedPipe[(Time, Event)])
        (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time, (Key, (Value, Option[Joined])))] = {
        val kvPipe = fm.apply(env, timeEv).map { case (t, (k,v)) => (t,k,v) }
        service.leftJoin(env, kvPipe).map { case (t,k,v) => (t,(k,v)) }
      }
    }
}
