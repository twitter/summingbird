/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.service

import com.twitter.util.Future

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.scalding.ScaldingEnv

import com.twitter.scalding.{Mode, TypedPipe}
import cascading.flow.FlowDef

import java.io.{Closeable, Serializable}

/** Represents an external service against which you can do a left join.
 * In the future, any Sink will be supported as a Service against which
 * we can join the previous batch (for consistency), but if you can
 * manage the consistency yourself, go crazy with this interface!
 */
trait BaseService[Key, JoinedValue] extends Serializable with Closeable {
  override def close { }
}

trait OfflineService[Key, JoinedValue] extends BaseService[Key, JoinedValue] {
  def leftJoin[Time,Value](env: ScaldingEnv, pipe: TypedPipe[(Time, Key, Value)])
    (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time, Key, (Value, Option[JoinedValue]))]
}

trait CompoundService[Key, Joined] extends ReadableStore[Key, Joined] with OfflineService[Key, Joined]

object CompoundService {
  implicit def fromStore[K, J](on: ReadableStore[K,J]): CompoundService[K,J] =
    new CompoundService[K, J] {
      override def get(k: K) = on.get(k)
      override def multiGet(ks: Set[K]) = on.multiGet(ks)
      def leftJoin[Time, Value](env: ScaldingEnv, pipe: TypedPipe[(Time, K, Value)])
        (implicit fd: FlowDef, mode: Mode) =
          sys.error("Online only source cannot be used offline: " + on.getClass)
    }

  implicit def fromOffline[K,J](off: OfflineService[K,J]): CompoundService[K,J] =
    new CompoundService[K,J] {
      override def get(k: K) = sys.error("Offline only source cannot be used online: " + off.getClass)
      override def multiGet(ks: Set[K]) = sys.error("Offline only source cannot be used online: " + off.getClass)
      def leftJoin[Time, Value](env: ScaldingEnv, pipe: TypedPipe[(Time, K, Value)])
        (implicit fd: FlowDef, mode: Mode) = off.leftJoin(env, pipe)
  }
}
