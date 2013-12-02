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

package com.twitter.summingbird.akka
import _root_.akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.twitter.summingbird.batch.{BatchID, Timestamp}

case class Data(d: ConsistentHashable) extends ConsistentHashable {
  def consistentHashKey = d.consistentHashKey
}

sealed trait WireType[+T] extends ConsistentHashable {
  def toFMInput: (Timestamp, T)
}

case class TimedTick()


case class ValueType[+V](t: Timestamp, v: V) extends WireType[V] {
  def consistentHashKey = v
  def toFMInput: (Timestamp, V) = (t, v)
}
case class KeyValueType[+K, +V](t: Timestamp, k: K, v: V) extends WireType[(K, V)] {
  def consistentHashKey = k
  def toFMInput: (Timestamp, (K, V)) = (t, (k, v))
}
