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

package com.twitter.summingbird.service

import com.twitter.util.Future

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.scalding.ScaldingEnv

import com.twitter.scalding.{Mode, TypedPipe}
import cascading.flow.FlowDef

import java.io.Serializable

/**
  * Represents an external service against which you can do a left
  * join.  In the future, any Sink will be supported as a Service
  * against which we can join the previous batch (for consistency), but
  * if you can manage the consistency yourself, go crazy with this
  * interface!
  */

trait OfflineService[Key, JoinedValue] extends Serializable {
  def leftJoin[Time,Value](pipe: TypedPipe[(Time, Key, Value)])
    (implicit fd: FlowDef, mode: Mode): TypedPipe[(Time, Key, (Value, Option[JoinedValue]))]
}

class EmptyOfflineService[K, JoinedV] extends OfflineService[K, JoinedV] {
  def leftJoin[T, V](pipe: TypedPipe[(T, K, V)])(implicit fd: FlowDef, mode: Mode) =
    pipe.map { case (t, k, v) => (t, k, (v, None: Option[JoinedV])) }
}

case class CompoundService[Key, Joined](offline: OfflineService[Key, Joined], online: () => ReadableStore[Key, Joined])

object CompoundService {
  def fromOffline[K, J](offline: OfflineService[K, J]): CompoundService[K, J] =
    CompoundService(offline, () => ReadableStore.empty)
  def fromOnline[K, J](online: => ReadableStore[K, J]): CompoundService[K, J] =
    CompoundService(new EmptyOfflineService[K, J], () => online)
}
