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

package com.twitter.summingbird.scalding

import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.chill.InjectionPair
import com.twitter.scalding.{ Tool => STool, _ }
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.builder.{ FlatMapShards, StoreIntermediateData }
import com.twitter.summingbird.util.KryoRegistrationHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.GenericOptionsParser
import java.util.{ Date, HashMap => JHashMap, Map => JMap, TimeZone }

case class ScaldingSerialization[T](injectionPair: InjectionPair[T]) extends Serialization[Scalding, T]

trait ScaldingStore[K, V] extends Store[Scalding, K, V] {
  // Could tag the beginning time of the Batch, or the time up to
  // which that key had been aggregated.
  def read(batchID: BatchID): TypedPipe[(K, V)]

  /**
    * Accepts deltas along with their timestamps, returns triples of
    * (time, K, V(aggregated up to the time)).
    *
    * Same return as lookup on a ScaldingService.
    */
  def merge(batchID: BatchID, pipe: TypedPipe[(Long, K, V)]): TypedPipe[(Long, K, V)]
}

trait ScaldingService[K, V] extends Service[Scalding, K, V] {
  def lookup(batchID: BatchID): TypedPipe[(Long, K, V)]
}

object Scalding {
  implicit def ser[T](implicit inj: Injection[T, Array[Byte]], mf: Manifest[T]): Serialization[Scalding, T] = {
    ScaldingSerialization(InjectionPair(mf.erasure.asInstanceOf[Class[T]], inj))
  }

  def source[T](factory: PipeFactory[T])
    (implicit inj: Injection[T, Array[Byte]], manifest: Manifest[T], timeOf: TimeExtractor[T]) =
    Producer.source[Scalding, T, PipeFactory[T]](factory)
}

trait ScaldingStore[K, V] {
  def write(p: TypedPipe[(Long, K, V)])
    (implicit kOrd: Ordering[K], monoid: Monoid[V],
      flowDef: FlowDef, mode: Mode) = { }
}

class Scalding(jobName: String) extends Platform[Scalding] {
  /**
    * run(Summer(producer, store))
    *
    * - command line runner gives us the # of batches we want to run.
    * - the store needs to give us the current maximum batch.
    */
  def buildFlow[T](producer: Producer[Scalding, T], id: Option[String])
    (implicit flowDef: FlowDef, mode: Mode): BatchID => TypedPipe[(Long, T)] = {

    outerProducer match {
      case Summer(producer, _, _, _, _, _) => {
        assert(path.isEmpty, "Only a single Summer is supported at this time.")
        recurse(producer)
      }
      case IdentityKeyedProducer(producer) => buildFlow(producer, id)
      case NamedProducer(producer, newId)  => buildFlow(producer, id = Some(newId))
      case Source(source, ser, timeOf) => {
        source.asInstanceOf[PipeFactory[T]].apply(batchRange)
          .map { t =>
      }

      case OptionMappedProducer(producer, op) => {
        recurse(producer)
      }

      case FlatMappedProducer(producer, op) => {
        recurse(producer)
      }

      case LeftJoinedProducer(producer, svc) => {
        recurse(producer)
      }

      case MergedProducer(l, r) => {
        recurse(l) ++ recurse(r)
      }
    }
  }

  /**
    * Base hadoop config instances used by the Scalding platform.
    */
  def baseConfig = new Configuration

  def run[K, V](summer: Summer[Scalding, K, V]): Unit = {
    implicit val flowDef = new FlowDef
    implicit val mode = new Mode
  }
}
