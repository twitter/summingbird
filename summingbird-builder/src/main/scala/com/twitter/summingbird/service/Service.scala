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

import com.twitter.scalding.{Mode, TypedPipe}
import com.twitter.summingbird.scalding.{ ScaldingService, EmptyService, ScaldingEnv }
import cascading.flow.FlowDef

import java.io.Serializable

case class CompoundService[Key, Joined](
  offline: ScaldingService[Key, Joined],
  online: () => ReadableStore[Key, Joined]
)

object CompoundService {
  def fromOffline[K, J](offline: ScaldingService[K, J]): CompoundService[K, J] =
    CompoundService(offline, () => ReadableStore.empty)
  def fromOnline[K, J](online: => ReadableStore[K, J]): CompoundService[K, J] =
    CompoundService(new EmptyService[K, J], () => online)
}
