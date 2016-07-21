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

package com.twitter.summingbird.builder

import com.twitter.summingbird._
import com.twitter.bijection.Injection
import com.twitter.bijection.Conversion.asMethod
import com.twitter.scalding._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.storm.StormEnv
import com.twitter.summingbird.source._
import com.twitter.summingbird.scalding.store.VersionedStore
import com.twitter.tormenta.spout.Spout
import com.twitter.storehaus.JMapStore
import com.twitter.storehaus.algebra.StoreAlgebra
import org.scalatest.WordSpec
import java.util.Date
import scala.util.Try

object TestJob {
  import Dsl._
  import StoreAlgebra.enrich

  implicit val batcher: Batcher = Batcher.ofHours(1)

  implicit def keyPairInjection[T](implicit toS: Injection[T, String]): Injection[(T, BatchID), Array[Byte]] =
    Injection.build[(T, BatchID), Array[Byte]] {
      case (t, batchID) =>
        (t.as[String] + ":" + batchID.as[String]).as[Array[Byte]]
    } { bytes: Array[Byte] =>
      for {
        deser <- bytes.as[Try[String]]
        Array(tString, batchIDString) = deser.split(":")
        t <- tString.as[Try[T]]
        batchID <- batchIDString.as[Try[BatchID]]
      } yield (t, batchID)
    }
  implicit def valPairInjection[T](implicit toS: Injection[T, String]): Injection[(BatchID, T), Array[Byte]] =
    Injection.connect[(BatchID, T), (T, BatchID), Array[Byte]]

  def offlineStore = VersionedStore[Long, Int]("tmp")
  def onlineStore = new JMapStore[(Long, BatchID), Int].toMergeable
}

class TestJobWithOffline(env: Env) extends AbstractJob(env) {
  import TestJob._

  EventSource.fromOnline {
    Spout.fromTraversable(1 to 100)
  }.withTime(new Date(_))
    .map { i => (100L, i) }
    .groupAndSumTo(offlineStore)
}

class TestJobWithOnline(env: Env) extends AbstractJob(env) {
  import TestJob._

  EventSource.fromOnline {
    Spout.fromTraversable(1 to 100)
  }.withTime(new Date(_))
    .map { i => (100L, i) }
    .groupAndSumTo(onlineStore)
}

class BuilderJobTest extends WordSpec {
  "Builder API should NOT throw when building a storm job w/ onlineStore" in {
    AbstractJob(
      "com.twitter.summingbird.builder.TestJobWithOnline",
      StormEnv("name", Args(Array.empty[String]))
    )
  }

  "Builder API should throw when building a storm job w/ missing onlineStore" in {
    intercept[Exception] {
      AbstractJob(
        "com.twitter.summingbird.builder.TestJobWithOffline",
        StormEnv("name", Args(Array.empty[String]))
      )
    }
  }
}
