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

package com.twitter.summingbird.storm.builder

import com.twitter.summingbird.batch.Timestamp
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck._

object InjectionLaws extends Properties("InjectionTests") {
  implicit def ts: Arbitrary[Timestamp] =
    Arbitrary(Arbitrary.arbitrary[Long].map(Timestamp(_)))

  property("Item injection works") = forAll { in: String =>
    val inj = EdgeTypeInjections.Item[String]()
    inj.invert(inj(in)).get == in
  }
  property("KV injection works") = forAll { in: (String, List[Int]) =>
    val inj = EdgeTypeInjections.KeyValue[String, List[Int]]()
    inj.invert(inj(in)).get == in
  }
}
