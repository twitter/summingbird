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

/*
 * What we would like to pass around to describe a service.
 * That is its a factory that produces readable store's.
 *
 * The Function1 here is to allow cleaner diasy chaining of operations via andThen.
 */
trait OnlineServiceFactory[-K, +V] extends java.io.Serializable {
  def serviceStore: () => ReadableStore[K, V]
}
