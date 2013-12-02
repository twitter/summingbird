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


import com.twitter.bijection.{Injection, Inversion, AbstractInjection, InversionFailure}
import com.twitter.summingbird.batch.Timestamp
import backtype.storm.tuple.Tuple
import scala.util.{ Failure, Success, Try }

import java.util.{List => JList, ArrayList => JAList}

object StormInjections {
  def TupleInjection =  new AbstractInjection[JList[AnyRef], Tuple] {
      override def apply(b: JList[AnyRef]) = null
      override def invert(a: Tuple) = Success(a.getValues)
    }

  def SingleItemInjection[T] = Injection.buildCatchInvert[(Timestamp, T), JList[AnyRef]] { t =>
      val list = new JAList[AnyRef](1)
      list.add(t)
      list
    }
    {
      _.get(0).asInstanceOf[(Timestamp, T)]
    }

    def KeyValueInjection[K, V] = Injection.buildCatchInvert[(Timestamp, (K, V)), JList[AnyRef]] { item =>
      val (ts, (key, v)) = item
      val list = new JAList[AnyRef](2)
      list.add(key.asInstanceOf[AnyRef])
      list.add((ts, v))
      list
    }
    { v =>
      val key = v.get(0).asInstanceOf[K]
      val (ts, value) = v.get(1).asInstanceOf[(Timestamp, V)]
      (ts, (key, value))
    }
}
