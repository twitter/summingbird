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

import com.twitter.bijection.{ Injection, Inversion, AbstractInjection }
import java.util.{ List => JList, ArrayList => JAList }
import scala.util.Try

class SingleItemInjection[T] extends Injection[T, JList[AnyRef]] {

  override def apply(t: T) = {
    val list = new JAList[AnyRef](1)
    list.add(t.asInstanceOf[AnyRef])
    list
  }

  override def invert(vin: JList[AnyRef]) = Inversion.attempt(vin) { v =>
    v.get(0).asInstanceOf[T]
  }
}

class KeyValueInjection[K, V]
    extends Injection[(K, V), JList[AnyRef]] {

  override def apply(item: (K, V)) = {
    val (key, v) = item
    val list = new JAList[AnyRef](2)
    list.add(key.asInstanceOf[AnyRef])
    list.add(v.asInstanceOf[AnyRef])
    list
  }

  override def invert(vin: JList[AnyRef]) = Inversion.attempt(vin) { v =>
    val key = v.get(0).asInstanceOf[K]
    val value = v.get(1).asInstanceOf[V]
    (key, value)
  }
}
