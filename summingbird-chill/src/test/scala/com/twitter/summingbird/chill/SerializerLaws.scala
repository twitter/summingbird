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

package com.twitter.summingbird.chill

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._
import org.scalatest.WordSpec
import com.twitter.chill._

import com.twitter.summingbird.batch._

object SerializationLaws extends Properties("SerializationLaws") {

  implicit val batchId: Arbitrary[BatchID] =
    Arbitrary(Arbitrary.arbitrary[Long].map(BatchID(_)))

  implicit val timestamp: Arbitrary[Timestamp] =
    Arbitrary(Arbitrary.arbitrary[Long].map(Timestamp(_)))

  implicit def batchSer: KSerializer[BatchID] = new BatchIDSerializer
  implicit def timestampSer: KSerializer[Timestamp] = new TimestampSerializer

  def round[T](ser: KSerializer[T], t: T): T = {
    val kinst = (new ScalaKryoInstantiator)
      .withRegistrar({ (k: Kryo) => k.register(t.getClass, ser); () })
    KryoPool.withBuffer(1, kinst, 100, 10000).deepCopy(t)
  }

  def roundTrips[T: Arbitrary: KSerializer: Equiv] = forAll { (t: T) =>
    val kser = implicitly[KSerializer[T]]
    Equiv[T].equiv(round(kser, t), t)
  }

  property("BatchID roundtrips with Kryo") = roundTrips[BatchID]
  property("Timestamp roundtrips with Kryo") = roundTrips[Timestamp]
}
