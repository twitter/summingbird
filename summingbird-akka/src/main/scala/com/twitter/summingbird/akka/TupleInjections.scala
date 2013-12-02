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

import com.twitter.bijection.{Injection, Inversion, AbstractInjection}
import com.twitter.summingbird.batch.Timestamp
import java.util.{List => JList, ArrayList => JAList}
import scala.util.Try
import com.twitter.summingbird.batch.{BatchID, Timestamp}
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.online.executor.DataInjection

import scala.util.{Try, Success, Failure}


class SingleItemInjection[T] extends DataInjection[T, WireType[T]] {
  override def apply(t: (Timestamp, T)) = ValueType(t._1, t._2)

  override def fields = List()
  override def invert(vin: WireType[T]) = vin match {
    case ValueType(t, v) => Success((t, v.asInstanceOf[T]))
    case _ => Failure(new Exception("Not the correct type"))
  }
}

class KeyValueInjection[K, V] extends DataInjection[(K, V), WireType[(K, V)]] {
  override def fields = List()
  override def apply(t: (Timestamp, (K, V))) = KeyValueType(t._1, t._2._1, t._2._2)

  override def invert(vin: WireType[(K, V)]) = vin match {
    case KeyValueType(t, k, v) => Success((t, (k.asInstanceOf[K], v.asInstanceOf[V])))
    case _ => Failure(new Exception("Not the correct type"))
  }
}
