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
import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveUpper, InclusiveUpper }
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

class Scalding(jobName: String, var state: WaitingState[Date], mode: Mode,
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

  /** Return a PipeFactory that can cover as much as possible of the time range requested,
   * but the output state gives the actual, non-empty, interval that can be produced
   */
  private def buildFlow[T](producer: Producer[Scalding, T],
    id: Option[String],
    fanOuts: Set[Producer[Scalding, _]],
    built: Map[Producer[Scalding, _], PipeFactory[_]]): (PipeFactory[T], Map[Producer[Scalding, _], PipeFactory[_]])  = {

    /**
     * The scalding Typed-API does not deal with TypedPipes with fanout,
     * it just computes both branches twice. We call this function
     * to force all Producers that have fanOut greater than 1
     * to render intermediate cascading pipes so that
     * cascading will optimize them properly
     *
     * TODO fix this in scalding
     * https://github.com/twitter/scalding/issues/513
     */
    def forceNode[U](p: PipeFactory[U]): PipeFactory[U] =
      if(fanOuts(producer))
        p.map { flowP =>
          flowP.map { pipe =>
            // For a cascading node:
            import com.twitter.scalding.Dsl._
            TypedPipe.from[(Long,U)](pipe.toPipe((0,1)), (0,1))
          }
        }
      else
        p

    built.get(producer) match {
      case Some(pf) => (pf.asInstanceOf[PipeFactory[T]], built)
      case None =>
        val (pf: PipeFactory[T], m) = producer match {
          case Source(src, manifest) => {
            val shards = getOrElse(id, FlatMapShards.default).count
            val srcPf = if (shards <= 1)
              src
            else
              // TODO (https://github.com/twitter/summingbird/issues/89):
              // TODO (https://github.com/twitter/scalding/issues/512):
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
            (srcPf, built)
          }
          case IdentityKeyedProducer(producer) =>
            buildFlow(producer, id, fanOuts, built)
          case NamedProducer(producer, newId)  =>
            buildFlow(producer, id = Some(newId), fanOuts, built)
          case Summer(producer, store, monoid) =>
            /*
             * The store may already have materialized values, so we don't need the whole
             * input history, but to produce NEW batches, we may need some input.
             * So, we pass the full PipeFactory to to the store so it can request only
             * the time ranges that it needs.
             */
            val (in, m) = buildFlow(producer, id, fanOuts, built)
            (store.merge(in, monoid,
              getOrElse(id, NonCommutative),
              getOrElse(id, Reducers.default).count), m)
          case LeftJoinedProducer(left, service) =>
            /**
             * There is no point loading more from the left than the service can
             * join with, so we pass in the left PipeFactory so that the service
             * can compute how wuch it can actually handle and only load that much
             */
            val (pf, m) = buildFlow(left, id, fanOuts, built)
            (service.lookup(pf), m)
          case WrittenProducer(producer, sink) =>
            val (pf, m) = buildFlow(producer, id, fanOuts, built)
            (sink.write(pf), m)
          case OptionMappedProducer(producer, op, manifest) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = buildFlow(producer, id, fanOuts, built)
            (fmp.map { flowP =>
              flowP.map { typedPipe =>
                // TODO
                // (https://github.com/twitter/summingbird/issues/90):
                // make Function1 instances outside to avoid the closure +
                // serialization issues
                typedPipe.flatMap { case (time, item) =>
                  op(item).map { (time, _) }.toIterable
                }
              }
            }, m)
          case FlatMappedProducer(producer, op) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = buildFlow(producer, id, fanOuts, built)
            (fmp.map { flowP =>
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
            }, m)
          case MergedProducer(l, r) => {
            val (pfl, ml) = buildFlow(l, id, fanOuts, built)
            val (pfr, mr) = buildFlow(r, id, fanOuts, ml)
            val merged = for {
              // concatenate errors (++) and find the intersection (&&) of times
              leftAndRight <- pfl.join(pfr,
                { (lerr: List[FailureReason], rerr: List[FailureReason]) => lerr ++ rerr },
                { case ((tsl, leftFM), (tsr, _)) => (tsl && tsr, leftFM) })
              merged = Scalding.merge(leftAndRight._1, leftAndRight._2)
              maxAvailable <- StateWithError.getState // read the latest state, which is the time
            } yield Scalding.limitTimes(maxAvailable._1, merged)
            (merged, mr)
          }
        }
        // Make sure that we end any chains of nodes at fanned out nodes:
        val res = forceNode(pf)
        (res, built + (producer -> res))
    }
  }

  def transformConfig(base: Configuration): Configuration = updateConf(base)
  def withConfigUpdater(fn: Configuration => Configuration): Scalding =
    new Scalding(jobName, state, mode, fn, options)

  def plan[T](prod: Producer[Scalding, T]): PipeFactory[T] = {
    val dep = Dependants(prod)
    val fanOutSet =
      Producer.transitiveDependenciesOf(prod)
        .filter(dep.fanOut(_).exists(_ > 1))
    buildFlow(prod, None, fanOutSet, Map.empty)._1
  }

  def run(pf: PipeFactory[_]) {
    import ConfigBijection._

    val runningState = state.begin
    val timeSpan = runningState.part.mapNonDecreasing(_.getTime)
    pf((timeSpan, mode)) match {
      case Left(errs) =>
        println("ERROR")
        errs.foreach { println(_) }
        state = runningState.fail(new Exception(errs.toString))
      case Right(((ts, mode), flowDefMutator)) =>
        val flowDef = new FlowDef
        flowDef.setName(jobName)
        val outputPipe = flowDefMutator((flowDef, mode))
        val conf = transformConfig(new Configuration)
          .as[Map[String, AnyRef]]
          .toMap[AnyRef, AnyRef]

        // Now we have a populated flowDef, time to let Cascading do it's thing:
        try {
          mode.newFlowConnector(conf).connect(flowDef).complete
          val nextTime = ts match {
            case Intersection(_, ExclusiveUpper(up)) => up
            case Intersection(_, InclusiveUpper(up)) => up + 1L
            case _ => sys.error("We should always be running for a finite interval")
          }
          state = runningState.succeed(ts.mapNonDecreasing(new Date(_)))
        }
        catch {
          case (e: Throwable) => {
            state = runningState.fail(e)
            throw e
          }
        }
    }
  }
}
