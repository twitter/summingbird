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
import com.twitter.algebird.{ Universe, Empty, Interval, Intersection, InclusiveLower, ExclusiveLower, ExclusiveUpper, InclusiveUpper }
import com.twitter.algebird.monad.{ StateWithError, Reader }
import com.twitter.algebird.Monad.operators // map/flatMap for monads
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.Injection
import com.twitter.scalding.{ Tool => STool, Source => SSource, _ }
import com.twitter.summingbird._
import com.twitter.summingbird.scalding.option.{ FlatMapShards, Reducers }
import com.twitter.summingbird.batch._
import com.twitter.summingbird.option._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.serializer.{Serialization => HSerialization}
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.GenericOptionsParser
import java.util.{ Date, HashMap => JHashMap, Map => JMap, TimeZone }
import cascading.flow.{FlowDef, Flow}
import com.twitter.scalding.Mode

import scala.util.control.Exception.allCatch

object Scalding {
  implicit val dateRangeInjection: Injection[DateRange, Interval[Time]] = Injection.build {
    (dr: DateRange) => {
      val DateRange(l, u) = dr
      Interval.leftClosedRightOpen(l.timestamp, u.timestamp + 1L)
    }
  } {
    case Intersection(lb, ub) =>
      val low = lb match {
        case InclusiveLower(l) => l
        case ExclusiveLower(l) => l+1L
      }
      val high = ub match {
        case InclusiveUpper(u) => u
        case ExclusiveUpper(u) => u-1L
      }
      Some(DateRange(RichDate(low), RichDate(high)))
    case _ => None
  }


  def emptyFlowProducer[T]: FlowProducer[TypedPipe[T]] = {
    import Dsl._
    Reader({implicit fdm: (FlowDef, Mode) => TypedPipe.from(IterableSource(Iterable.empty[T])) })
  }

  def intersect(dr1: DateRange, dr2: DateRange): Option[DateRange] =
    (dr1.as[Interval[Time]] && (dr2.as[Interval[Time]])).as[Option[DateRange]]

  /** Given a constructor function, computes the maximum available range
   * of time or gives an error.
   *
   * Works by calling validateTaps on the Mappable, so if that does not work correctly
   * this will be incorrect.
   */
  def minify(mode: Mode, desired: DateRange)(factory: (DateRange) => SSource):
    Either[List[FailureReason], DateRange] = {
      try {
        val available = (mode, factory(desired)) match {
          case (hdfs: Hdfs, ts: TimePathedSource) => minify(hdfs, ts)
          case _ => bisectingMinify(mode, desired)(factory)
        }
        available.flatMap { intersect(desired, _) }
          .map(Right(_))
          .getOrElse(Left(List("available: " + available + ", desired: " + desired)))
      }
      catch { case t: Throwable => toTry(t) }
    }

  private def bisectingMinify(mode: Mode, desired: DateRange)(factory: (DateRange) => SSource): Option[DateRange] = {
    def isGood(end: Long): Boolean = allCatch.opt(factory(DateRange(desired.start, RichDate(end))).validateTaps(mode)).isDefined
    val DateRange(start, end) = desired
    if(isGood(start.timestamp)) {
      // The invariant is that low isGood, low < upper, and upper isGood == false
      @annotation.tailrec
      def findEnd(low: Long, upper: Long): Long =
        if (low == (upper - 1L))
          low
        else {
          // mid must be > low because upper >= low + 2
          val mid = low + (upper - low)/2
          if(isGood(mid))
            findEnd(mid, upper)
          else
            findEnd(low, mid)
        }

      if(isGood(end.timestamp)) Some(desired)
      else Some(DateRange(desired.start, RichDate(findEnd(start.timestamp, end.timestamp))))
    }
    else {
      // No good data
      None
    }
  }

  // TODO move this to scalding:
  // https://github.com/twitter/scalding/issues/529
  private def minify(mode: Hdfs, ts: TimePathedSource): Option[DateRange] = {
    import ts.{pattern, dateRange, tz}

    def combine(prev: DateRange, next: DateRange) = DateRange(prev.start, next.end)

    def pathIsGood(p : String): Boolean = {
      val path = new Path(p)
      Option(path.getFileSystem(mode.conf).globStatus(path))
        .map(_.length > 0)
        .getOrElse(false)
    }

    List("%1$tH" -> Hours(1), "%1$td" -> Days(1)(tz),
      "%1$tm" -> Months(1)(tz), "%1$tY" -> Years(1)(tz))
      .find { unitDur : (String, Duration) => pattern.contains(unitDur._1) }
      .map { unitDur =>
        dateRange.each(unitDur._2)
          .map { dr : DateRange =>
            val path = String.format(pattern, dr.start.toCalendar(tz))
            (dr, path)
          }
      }
      .getOrElse(List((dateRange, pattern))) // This must not have any time after all
      .takeWhile { case (_, path) => pathIsGood(path) }
      .map(_._1)
      .reduceOption { combine(_, _) }
  }

  /** This uses minify to find the smallest subset we can run.
   * If you don't want this behavior, then use pipeFactoryExact which
   * either produces all the DateRange or the whole job fails.
   */
  def pipeFactory[T](factory: (DateRange) => Mappable[T])
    (implicit timeOf: TimeExtractor[T]): PipeFactory[T] =
    StateWithError[(Interval[Time], Mode), List[FailureReason], FlowToPipe[T]]{
      (timeMode: (Interval[Time], Mode)) => {
        val (timeSpan, mode) = timeMode

        toDateRange(timeSpan).right.flatMap { dr =>
          minify(mode, dr)(factory)
            .right.map { newDr =>
              val newIntr = newDr.as[Interval[Time]]
              val mappable = factory(newDr)
              ((newIntr, mode), Reader { (fdM: (FlowDef, Mode)) =>
                TypedPipe.from(mappable)(fdM._1, fdM._2, mappable.converter) // converter is removed in 0.9.0
                  .flatMap { t =>
                    val time = timeOf(t)
                    if(newIntr(time)) Some((time, t)) else None
                  }
              })
            }
        }
      }
    }

  def pipeFactoryExact[T](factory: (DateRange) => Mappable[T])
    (implicit timeOf: TimeExtractor[T]): PipeFactory[T] =
    StateWithError[(Interval[Time], Mode), List[FailureReason], FlowToPipe[T]]{
      (timeMode: (Interval[Time], Mode)) => {
        val (timeSpan, mode) = timeMode

        toDateRange(timeSpan).right.map { dr =>
          val mappable = factory(dr)
          ((timeSpan, mode), Reader { (fdM: (FlowDef, Mode)) =>
            mappable.validateTaps(fdM._2) //This can throw, but that is what this caller wants
            TypedPipe.from(mappable)(fdM._1, fdM._2, mappable.converter) // converter is removed in 0.9.0
              .flatMap { t =>
                val time = timeOf(t)
                if(timeSpan(time)) Some((time, t)) else None
              }
          })
        }
      }
    }

  def sourceFromMappable[T: TimeExtractor: Manifest](
    factory: (DateRange) => Mappable[T]): Producer[Scalding, T] =
    Producer.source[Scalding, T](pipeFactory(factory))

  def toDateRange(timeSpan: Interval[Time]): Try[DateRange] =
      timeSpan.as[Option[DateRange]]
        .map { Right(_) }
        .getOrElse(Left(List("only finite time ranges are supported by scalding: " + timeSpan.toString)))

  /** The typed API (currently, as of 0.9.0) hides all flatMaps
   * from cascading, so if you fork a pipe, the whole input
   * is recomputed. This function forces a cascading node
   * so that cascading can see that any forks of the output
   * share a common prefix of operations
   */
  def forcePipe[U](pipe: TimedPipe[U]): TimedPipe[U] = {
    import com.twitter.scalding.Dsl._
    TypedPipe.from[(Long,U)](pipe.toPipe((0,1)), (0,1))
  }

  def limitTimes[T](range: Interval[Time], in: FlowToPipe[T]): FlowToPipe[T] =
    in.map { pipe => pipe.filter { case (time, _) => range(time) } }

  def merge[T](left: FlowToPipe[T], right: FlowToPipe[T]): FlowToPipe[T] =
    for { l <- left; r <- right } yield (l ++ r)

  /** Memoize the inner reader
   *  This is not a performance optimization, but a correctness one applicable
   *  to some cases (namely any function that mutates the FlowDef or does IO).
   *  Though we are working in a referentially transparent manner, the application
   *  of the function inside the PipeFactory (the Reader) mutates the FlowDef.
   *  For a fixed PipeFactory, we only want to mutate a given FlowDef once.
   *  If we memoize with this function, it guarantees that the PipeFactory
   *  is idempotent.
   * */
  def memoize[T](pf: PipeFactory[T]): PipeFactory[T] = {
    val memo = new Memo[T]
    pf.map { rdr =>
      Reader({ i => memo.getOrElseUpdate(i, rdr) })
    }
  }


  private def getOrElse[T: Manifest](options: Map[String, Options], idOpt: Option[String], default: T): T =
    (for {
      id <- idOpt
      innerOpts <- options.get(id)
      option <- innerOpts.get[T]
    } yield option).getOrElse(default)

  /** Return a PipeFactory that can cover as much as possible of the time range requested,
   * but the output state gives the actual, non-empty, interval that can be produced
   */
  private def buildFlow[T](options: Map[String, Options],
    producer: Producer[Scalding, T],
    id: Option[String],
    fanOuts: Set[Producer[Scalding, _]],
    built: Map[Producer[Scalding, _], PipeFactory[_]]): (PipeFactory[T], Map[Producer[Scalding, _], PipeFactory[_]]) = {

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
          flowP.map { Scalding.forcePipe(_) }
        }
      else
        p

    built.get(producer) match {
      case Some(pf) => (pf.asInstanceOf[PipeFactory[T]], built)
      case None =>
        val (pf, m) = producer match {
          case Source(src, manifest) => {
            val shards = getOrElse(options, id, FlatMapShards.default).count
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
            buildFlow(options, producer, id, fanOuts, built)
          case NamedProducer(producer, newId)  =>
            buildFlow(options, producer, id = Some(newId), fanOuts, built)
          case Summer(producer, store, monoid) =>
            /*
             * The store may already have materialized values, so we don't need the whole
             * input history, but to produce NEW batches, we may need some input.
             * So, we pass the full PipeFactory to to the store so it can request only
             * the time ranges that it needs.
             */
            val (in, m) = buildFlow(options, producer, id, fanOuts, built)
            val isCommutative = getOrElse(options, id, MonoidIsCommutative.default)
            (store.merge(in, monoid,
              isCommutative.commutativity,
              getOrElse(options, id, Reducers.default).count), m)
          case LeftJoinedProducer(left, service) =>
            /**
             * There is no point loading more from the left than the service can
             * join with, so we pass in the left PipeFactory so that the service
             * can compute how wuch it can actually handle and only load that much
             */
            val (pf, m) = buildFlow(options, left, id, fanOuts, built)
            (service.lookup(pf), m)
          case WrittenProducer(producer, sink) =>
            val (pf, m) = buildFlow(options, producer, id, fanOuts, built)
            (sink.write(pf), m)
          case OptionMappedProducer(producer, op, manifest) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = buildFlow(options, producer, id, fanOuts, built)
            (fmp.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap { case (time, item) =>
                  op(item).map { (time, _) }.toIterable
                }
              }
            }, m)
          case FlatMappedProducer(producer, op) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = buildFlow(options, producer, id, fanOuts, built)
            (fmp.map { flowP =>
              flowP.map { typedPipe =>
                // TODO
                // (https://github.com/twitter/summingbird/issues/89):
                // remove toIterable in scalding 0.9.0
                typedPipe.flatMap { case (time, item) =>
                  op(item).toIterable.view.map { (time, _) }
                }
              }
            }, m)
          case MergedProducer(l, r) => {
            val (pfl, ml) = buildFlow(options, l, id, fanOuts, built)
            val (pfr, mr) = buildFlow(options, r, id, fanOuts, ml)
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
        val res = memoize(forceNode(pf))
        (res.asInstanceOf[PipeFactory[T]], m + (producer -> res))
    }
  }

  def plan[T](options: Map[String, Options], prod: Producer[Scalding, T]): PipeFactory[T] = {
    val dep = Dependants(prod)
    val fanOutSet =
      Producer.transitiveDependenciesOf(prod)
        .filter(dep.fanOut(_).exists(_ > 1)).toSet
    buildFlow(options, prod, None, fanOutSet, Map.empty)._1
  }

  /** Use this method to interop with existing scalding code
   * Note this may return a smaller DateRange than you ask for
   * If you need an exact DateRange see toPipeExact.
   */
  def toPipe[T](dr: DateRange,
    prod: Producer[Scalding, T],
    opts: Map[String, Options] = Map.empty)(implicit fd: FlowDef, mode: Mode): Try[(DateRange, TypedPipe[(Long, T)])] = {
      val ts = dr.as[Interval[Time]]
      val pf = plan(opts, prod)
      toPipe(ts, fd, mode, pf).right.map { case (ts, pipe) =>
        (ts.as[Option[DateRange]].get, pipe)
      }
  }

  /** Use this method to interop with existing scalding code that expects
   * to schedule an exact DateRange or fail.
   */
  def toPipeExact[T](dr: DateRange,
    prod: Producer[Scalding, T],
    opts: Map[String, Options] = Map.empty)(implicit fd: FlowDef, mode: Mode): Try[TypedPipe[(Long, T)]] = {
      val ts = dr.as[Interval[Time]]
      val pf = plan(opts, prod)
      toPipeExact(ts, fd, mode, pf)
    }

  def toPipe[T](timeSpan: Interval[Time],
    flowDef: FlowDef,
    mode: Mode,
    pf: PipeFactory[T]): Try[(Interval[Time], TimedPipe[T])] =
    pf((timeSpan, mode))
      .right
      .map { case (((ts, m), flowDefMutator)) => (ts, flowDefMutator((flowDef, m))) }

  def toPipeExact[T](timeSpan: Interval[Time],
    flowDef: FlowDef,
    mode: Mode,
    pf: PipeFactory[T]): Try[TimedPipe[T]] =
    pf((timeSpan, mode))
      .right
      .flatMap { case (((ts, m), flowDefMutator)) =>
        if(ts != timeSpan) Left(List("Could not load all of %s, only %s".format(ts, timeSpan)))
        else Right(flowDefMutator((flowDef, m)))
      }
}

// Jank to get around serialization issues
class Memo[T] extends java.io.Serializable {
  @transient private val mmap = scala.collection.mutable.Map[(FlowDef,Mode), TimedPipe[T]]()
  def getOrElseUpdate(in: (FlowDef,Mode), rdr: Reader[(FlowDef,Mode),TimedPipe[T]]): TimedPipe[T] =
    mmap.getOrElseUpdate(in, rdr(in))
}

/** Use this option to write the logical graph that cascading
 * produces before Map/Reduce planning.
 * Use the job name as the key
 */
case class WriteDot(filename: String)

/** Use this option to write map/reduce graph
 * that cascading produces
 * Use the job name as the key
 */
case class WriteStepsDot(filename: String)

class Scalding(
  jobName: String,
  @transient options: Map[String, Options] = Map.empty)
    extends Platform[Scalding] {

  type Source[T] = PipeFactory[T]
  type Store[K, V] = ScaldingStore[K, V]
  type Sink[T] = ScaldingSink[T]
  type Service[K, V] = ScaldingService[K, V]
  type Plan[T] = PipeFactory[T]

  def plan[T](prod: Producer[Scalding, T]): PipeFactory[T] =
    Scalding.plan(options, prod)

  protected def ioSerializations: List[Class[_ <: HSerialization[_]]] = List(
    classOf[org.apache.hadoop.io.serializer.WritableSerialization],
    classOf[cascading.tuple.hadoop.TupleSerialization],
    classOf[com.twitter.summingbird.scalding.SummingbirdKryoHadoop]
  )

  private def setIoSerializations(m: Mode): Unit =
    m match {
      case Hdfs(_, conf) =>
        conf.set("io.serializations", ioSerializations.map { _.getName }.mkString(","))
      case _ => ()
    }

  // This is a side-effect-free computation that is called by run
  def toFlow(timeSpan: Interval[Time], mode: Mode, pf: PipeFactory[_]): Try[(Interval[Time], Flow[_])] = {
    val flowDef = new FlowDef
    flowDef.setName(jobName)
    Scalding.toPipe(timeSpan, flowDef, mode, pf)
      .right
      .flatMap { case (ts, pipe) =>
        // Now we have a populated flowDef, time to let Cascading do it's thing:
        try {
          Right((ts, mode.newFlowConnector(mode.config).connect(flowDef)))
        } catch {
          case (e: Throwable) => toTry(e)
        }
    }
  }

  def run(state: WaitingState[Date], mode: Mode, pf: Producer[Scalding, _]): WaitingState[Date] =
    run(state, mode, plan(pf))

  def run(state: WaitingState[Date], mode: Mode, pf: PipeFactory[_]): WaitingState[Date] = {
    setIoSerializations(mode)
    val runningState = state.begin
    val timeSpan = runningState.part.mapNonDecreasing(_.getTime)
    toFlow(timeSpan, mode, pf) match {
      case Left(errs) =>
        runningState.fail(FlowPlanException(errs))
      case Right((ts,flow)) =>
        try {
          options.get(jobName).foreach { jopt =>
            jopt.get[WriteDot].foreach { o => flow.writeDOT(o.filename) }
            jopt.get[WriteStepsDot].foreach { o => flow.writeStepsDOT(o.filename) }
          }
          flow.complete
          if (flow.getFlowStats.isSuccessful)
            runningState.succeed(ts.mapNonDecreasing(new Date(_)))
          else
            runningState.fail(new Exception("Flow did not complete."))
        } catch {
          case (e: Throwable) => {
            runningState.fail(e)
          }
        }
    }
  }
}
