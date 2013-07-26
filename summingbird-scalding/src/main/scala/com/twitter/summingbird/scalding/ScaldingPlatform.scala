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

import com.twitter.algebird.{ Monoid, Semigroup, Monad }
import com.twitter.algebird.monad.{ StateWithError, Reader }
import com.twitter.algebird.Monad.operators // map/flatMap for monads
import com.twitter.bijection.Conversion.asMethod
import com.twitter.scalding.{ Tool => STool, _ }
import com.twitter.summingbird._
import com.twitter.summingbird.builder.{ FlatMapShards, Reducers }
import com.twitter.summingbird.batch._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.GenericOptionsParser
import java.util.{ Date, HashMap => JHashMap, Map => JMap, TimeZone }
import cascading.flow.FlowDef
import com.twitter.scalding.Mode

import scala.util.control.Exception.allCatch

object Scalding {
  def pipeFactory[T](factory: (DateRange) => Mappable[T])
    (implicit timeOf: TimeExtractor[T]): PipeFactory[T] =
    StateWithError[(Interval[Time], Mode), List[FailureReason], FlowToPipe[T]]{
      (timeMode: (Interval[Time], Mode)) => {
        val (timeSpan, mode) = timeMode

        val timeSpanAsDateRange: Either[List[FailureReason], DateRange] =
          timeSpan match {
            case Intersection(InclusiveLower(low), ExclusiveUpper(up)) =>
              Right(DateRange(RichDate(low), RichDate(up - 1L))) // these are inclusive until 0.9.0
            case _ => Left(List("only finite time ranges are supported by scalding: " + timeSpan.toString))
          }

        timeSpanAsDateRange.right.flatMap { dr =>
          val mappable = factory(dr)
          // Check that this input is available:
            (allCatch.either(mappable.validateTaps(mode)) match {
              case Left(x) => Left(List(x.toString))
              case Right(()) => Right(())
            })
            .right.map { (_:Unit) =>
              (timeMode, Reader { (fdM: (FlowDef, Mode)) =>
                TypedPipe.from(mappable)(fdM._1, fdM._2, mappable.converter) // converter is removed in 0.9.0
                  .flatMap { t =>
                  // Todo: get the closure out of here for serialization safety
                  val time = timeOf(t)
                  if(timeSpan(time)) Some((time, t)) else None
                }
              })
            }
        }
      }
    }

  def sourceFromMappable[T: TimeExtractor: Manifest](
    factory: (DateRange) => Mappable[T]): Producer[Scalding, T] =
    Producer.source[Scalding, T](pipeFactory(factory))

  def limitTimes[T](range: Interval[Time], in: FlowToPipe[T]): FlowToPipe[T] =
    in.map { pipe => pipe.filter { case (time, _) => range(time) } }

  def merge[T](left: FlowToPipe[T], right: FlowToPipe[T]): FlowToPipe[T] =
    for { l <- left; r <- right } yield (l ++ r)
}

class Scalding(jobName: String, timeSpan: Interval[Time], mode: Mode,
  updateConf: Configuration => Configuration = identity,
  options: Map[String, Options] = Map.empty)
    extends Platform[Scalding] {
  type Source[T] = PipeFactory[T]
  type Store[K, V] = ScaldingStore[K, V]
  type Sink[T] = ScaldingSink[T]
  type Service[K, V] = ScaldingService[K, V]
  type Plan[T] = PipeFactory[T]
  /**
    * run(Summer(producer, store))
    *
    * - command line runner gives us the # of batches we want to run.
    * - the store needs to give us the current maximum batch.
    */

  private def getOrElse[T: Manifest](idOpt: Option[String], default: T): T =
    (for {
      id <- idOpt
      innerOpts <- options.get(id)
      option <- innerOpts.get[T]
    } yield option).getOrElse(default)

  private def buildSummer[K, V](summer: Summer[Scalding, K, V], id: Option[String]): PipeFactory[(K, V)] = {
    val Summer(producer, store, monoid) = summer
    /*
     * The store may already have materialized values, so we don't need the whole
     * input history, but to produce NEW batches, we may need some input.
     * So, we pass the full PipeFactory to to the store so it can request only
     * the time ranges that it needs.
     */
    store.merge(
      buildFlow(producer, id), monoid,
      getOrElse(id, NonCommutative),
      getOrElse(id, Reducers.default).count
    )
  }

  private def buildJoin[K, V, JoinedV](joined: LeftJoinedProducer[Scalding, K, V, JoinedV],
    id: Option[String]): PipeFactory[(K, (V, Option[JoinedV]))] = {
    val LeftJoinedProducer(left, service) = joined
    /**
     * There is no point loading more from the left than the service can
     * join with, so we pass in the left PipeFactory so that the service
     * can compute how wuch it can actually handle and only load that much
     */
    service.lookup(buildFlow(left, id))
  }

  /** Return a PipeFactory that can cover as much as possible of the time range requested,
   * but the output state gives the actual, non-empty, interval that can be produced
   */
  def buildFlow[T](producer: Producer[Scalding, T], id: Option[String]): PipeFactory[T] = {
    producer match {
      case Source(src, manifest) => {
        val shards = getOrElse(id, FlatMapShards.default).count
        if (shards <= 1)
          src
        else
          // TODO (https://github.com/twitter/summingbird/issues/89):
          // switch this to groupRandomly when it becomes available in
          // the typed API
          src.map { flowP =>
            flowP.map { pipe =>
              pipe.groupBy { event => new java.util.Random().nextInt(shards) }
                .mapValues(identity(_)) // hack to get scalding to actually do the groupBy
                .withReducers(shards)
                .values
            }
          }
      }
      case IdentityKeyedProducer(producer) => buildFlow(producer, id)
      case NamedProducer(producer, newId)  => buildFlow(producer, id = Some(newId))
      case summer@Summer(producer, store, monoid) => buildSummer(summer, id)
      case joiner@LeftJoinedProducer(producer, svc) => buildJoin(joiner, id)
      case WrittenProducer(producer, sink) => sink.write(buildFlow(producer, id))
      case OptionMappedProducer(producer, op, manifest) =>
        // Map in two monads here, first state then reader
        buildFlow(producer, id).map { flowP =>
          flowP.map { typedPipe =>
            // TODO
            // (https://github.com/twitter/summingbird/issues/90):
            // make Function1 instances outside to avoid the closure +
            // serialization issues
            typedPipe.flatMap { case (time, item) =>
              op(item).map { (time, _) }.toIterable
            }
          }
        }
      case FlatMappedProducer(producer, op) =>
        // Map in two monads here, first state then reader
        buildFlow(producer, id).map { flowP =>
          flowP.map { typedPipe =>
            // TODO
            // (https://github.com/twitter/summingbird/issues/89):
            // remove toIterable in scalding 0.9.0

            // TODO
            // (https://github.com/twitter/summingbird/issues/90):
            // make Function1 instances outside to avoid the closure +
            // serialization issues
            typedPipe.flatMap { case (time, item) =>
              op(item).toIterable.view.map { (time, _) }
            }
          }
        }
      case MergedProducer(l, r) => {
        for {
          // concatenate errors (++) and find the intersection (&&) of times
          leftAndRight <- buildFlow(l, id).join(buildFlow(r, id),
            { (lerr: List[FailureReason], rerr: List[FailureReason]) => lerr ++ rerr },
            { case ((tsl, leftFM), (tsr, _)) => (tsl && tsr, leftFM) })
          merged = Scalding.merge(leftAndRight._1, leftAndRight._2)
          maxAvailable <- StateWithError.getState // read the latest state, which is the time
        } yield Scalding.limitTimes(maxAvailable._1, merged)
      }
    }
  }

  def transformConfig(base: Configuration): Configuration = updateConf(base)
  def withConfigUpdater(fn: Configuration => Configuration): Scalding =
    new Scalding(jobName, timeSpan, mode, fn, options)

  def plan[T](prod: Producer[Scalding, T]): PipeFactory[T] =
    buildFlow(prod, None)

  def run(pf: PipeFactory[_]) {
    import ConfigBijection._

    pf((timeSpan, mode)) match {
      case Left(errs) =>
        println("ERROR")
        errs.foreach { println(_) }
      case Right(((ts, mode), flowDefMutator)) =>
        val flowDef = new FlowDef
        flowDef.setName(jobName)
        val outputPipe = flowDefMutator((flowDef, mode))
        val conf = transformConfig(new Configuration)
          .as[Map[String, AnyRef]]
          .toMap[AnyRef, AnyRef]

        // Now we have a populated flowDef, time to let Cascading do it's thing:
        mode.newFlowConnector(conf).connect(flowDef).complete
        // TODO (https://github.com/twitter/summingbird/issues/81):
        // log that we have completed all of ts, and should start at
        // the upperbound
    }
  }
}
