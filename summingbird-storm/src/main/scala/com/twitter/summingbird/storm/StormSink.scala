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

import com.twitter.util.Future
import com.twitter.storehaus.{ Store, ReadableStore, WritableStore }

import com.twitter.summingbird.online._

trait StormSink[-T] extends java.io.Serializable {
  def toFn: T => Future[Unit]
}

class SinkFn[T](fn: => T => Future[Unit]) extends StormSink[T] {
  lazy val toFn = fn
}

class WritableStoreSink[K, V](writable: => WritableStore[K, V]) extends StormSink[(K, V)] {
  private lazy val store = writable // only construct it once
  def toFn = store.put(_)
}

/**
 * Used to do leftJoins of streams against other streams
 */
class StormBuffer[K, V](supplier: => Store[K, V]) extends StormSink[(K, V)] with OnlineServiceFactory[K, V] {
  private lazy val constructed = supplier // only construct it once
  def toFn = { (kv: (K, V)) => constructed.put((kv._1, Some(kv._2))) }
  def serviceStore: () => ReadableStore[K, V] = () => constructed
}
