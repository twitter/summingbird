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

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.scalding.Service

/**
 * Pairing of an online and offline service for use with an
 * OptionalPlatform2[Scalding, Storm].
 */
case class CompoundService[Key, Joined](
  offline: Option[Service[Key, Joined]],
  online: Option[() => ReadableStore[Key, Joined]])

object CompoundService {
  def apply[K, J](offline: Service[K, J], online: => ReadableStore[K, J]): CompoundService[K, J] =
    CompoundService(Some(offline), Some(() => online))
  def fromOffline[K, J](offline: Service[K, J]): CompoundService[K, J] =
    CompoundService(Some(offline), None)
  def fromOnline[K, J](online: => ReadableStore[K, J]): CompoundService[K, J] =
    CompoundService(None, Some(() => online))
}
