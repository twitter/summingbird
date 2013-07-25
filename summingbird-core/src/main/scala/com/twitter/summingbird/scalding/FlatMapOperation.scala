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

import cascading.flow.FlowDef

import com.twitter.scalding.{Mode, TypedPipe, Dsl}
import com.twitter.summingbird.FlatMapper
import com.twitter.summingbird.service.OfflineService
import com.twitter.summingbird.sink.OfflineSink

import java.io.Serializable

// Represents the logic in the flatMap offline
trait FlatMapOperation[Event,Key,Value] extends Serializable { self =>
  def apply(timeEv: TypedPipe[(Long, Event)])
    (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv): TypedPipe[(Long, (Key, Value))]

  def andThen[K2,V2](fmo: FlatMapOperation[(Key,Value), K2, V2]): FlatMapOperation[Event,K2,V2] =
    new FlatMapOperation[Event,K2,V2] {
      def apply(timeEv: TypedPipe[(Long, Event)])
        (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv) =
          fmo(self(timeEv))
    }
}

object FlatMapOperation {
  def apply[Event, Key, Value](fm: FlatMapper[Event, Key, Value]): FlatMapOperation[Event, Key, Value] =
    new FlatMapOperation[Event, Key, Value] {
      def apply(timeEv: TypedPipe[(Long, Event)])
        (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv): TypedPipe[(Long, (Key, Value))] = {
        timeEv.flatMap { case (t: Long, e: Event) =>
            // TODO remove toList when scalding supports TraversableOnce
            fm.encode(e).map { (t, _) }.toList
        }
      }
    }

  def combine[Event,Key,Value,Joined](
    fm: FlatMapOperation[Event, Key, Value],
    service: OfflineService[Key, Joined]
  ): FlatMapOperation[Event, Key, (Value, Option[Joined])] =
    new FlatMapOperation[Event, Key, (Value, Option[Joined])] {
      def apply(timeEv: TypedPipe[(Long, Event)])
        (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv):
          TypedPipe[(Long, (Key, (Value, Option[Joined])))] = {
        val kvPipe = fm.apply(timeEv).map { case (t, (k,v)) => (t,k,v) }
        service.leftJoin(kvPipe).map { case (t,k,v) => (t,(k,v)) }
      }
    }

  def write[Event, Key, Value, Written](
    fm: FlatMapOperation[Event, Key, Value],
    sink: OfflineSink[Written],
    conversion: ((Key, Value)) => TraversableOnce[Written]
  ): FlatMapOperation[Event, Key, Value] =
    new FlatMapOperation[Event, Key, Value] {
      def apply(timeEv: TypedPipe[(Long, Event)])
        (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv):
          TypedPipe[(Long, (Key, Value))] = {
        val encoded = fm(timeEv)
        // force this into a new TypedPipe so that cascading can see the node in the graph
        // If we don't do this, the fm function is applied twice.
        // This was causing a serious additional cost on the cluster
        import Dsl._
        val newPipe = TypedPipe.from[(Long, (Key, Value))](encoded.toPipe((0, 1)), (0, 1))
        sink.write(newPipe.flatMap {
          // TODO remove toList when scalding supports TraversableOnce
          case (time, pair) => conversion(pair).toList
        })
        newPipe
      }
    }
}
