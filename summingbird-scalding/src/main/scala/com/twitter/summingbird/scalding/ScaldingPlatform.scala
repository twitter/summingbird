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

import cascading.flow.Flow
import com.twitter.algebird.{ Monoid, Semigroup, Monad }
import com.twitter.algebird.{
  Universe,
  Empty,
  Interval,
  Intersection,
  InclusiveLower,
  ExclusiveLower,
  ExclusiveUpper,
  InclusiveUpper
}
import cascading.flow.{ FlowDef, Flow, FlowProcess }
import com.twitter.algebird.monad.{ StateWithError, Reader }
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.{ AbstractInjection, Injection }
import com.twitter.chill.IKryoRegistrar
import com.twitter.chill.java.IterableRegistrar
import com.twitter.scalding.Config
import com.twitter.scalding.Mode
import com.twitter.scalding.{ Tool => STool, Source => SSource, TimePathedSource => STPS, _ }
import com.twitter.summingbird._
import com.twitter.summingbird.batch._
import com.twitter.summingbird.batch.option.{ FlatMapShards, Reducers }
import com.twitter.summingbird.chill._
import com.twitter.summingbird.option._
import com.twitter.summingbird.scalding.batch.BatchedStore
import com.twitter.summingbird.scalding.source.{ TimePathedSource => BTimePathedSource }
import java.util.{ HashMap => JHashMap, Map => JMap, TimeZone }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.serializer.{ Serialization => HSerialization }
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.ToolRunner
import org.slf4j.LoggerFactory
import scala.util.{ Success, Try => STry, Failure }
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Scalding {
  @transient private val logger = LoggerFactory.getLogger(classOf[Scalding])

  def apply(jobName: String, options: Map[String, Options] = Map.empty) = {
    new Scalding(jobName, options, identity, List())
  }

  implicit val dateRangeInjection: Injection[DateRange, Interval[Timestamp]] =
    new AbstractInjection[DateRange, Interval[Timestamp]] {
      override def apply(dr: DateRange) = {
        val DateRange(l, u) = dr
        Intersection(InclusiveLower(Timestamp(l.timestamp)), ExclusiveUpper(Timestamp(u.timestamp + 1L)))
      }
      override def invert(in: Interval[Timestamp]) = in match {
        case Intersection(lb, ub) =>
          val low = lb match {
            case InclusiveLower(l) => l
            case ExclusiveLower(l) => l.next
          }
          val high = ub match {
            case InclusiveUpper(u) => u
            case ExclusiveUpper(u) => u.prev
          }
          Success(DateRange(low.toRichDate, high.toRichDate))
        case _ => Failure(new RuntimeException("Unbounded interval!"))
      }
    }

  def emptyFlowProducer[T]: FlowProducer[TypedPipe[T]] =
    Reader({ implicit fdm: (FlowDef, Mode) => TypedPipe.empty })

  def getCommutativity(names: List[String],
    options: Map[String, Options],
    s: Summer[Scalding, _, _]): Commutativity = {

    val commutativity = getOrElse(options, names, s, {
      val default = MonoidIsCommutative.default
      logger.warn("Store: %s has no commutativity setting. Assuming %s".format(names, default))
      default
    }).commutativity

    commutativity match {
      case Commutative =>
        logger.info("Store: {} is commutative", names)
      case NonCommutative =>
        logger.info("Store: {} is non-commutative (less efficient than commutative)", names)
    }

    commutativity
  }

  def intersect(dr1: DateRange, dr2: DateRange): Option[DateRange] =
    (dr1.as[Interval[Timestamp]] && (dr2.as[Interval[Timestamp]])).as[Option[DateRange]]

  /**
   * Given a constructor function, computes the maximum available range
   * of time or gives an error.
   *
   * Works by calling validateTaps on the Mappable, so if that does not work correctly
   * this will be incorrect.
   */
  def minify(mode: Mode, desired: DateRange)(factory: (DateRange) => SSource): Either[List[FailureReason], DateRange] =
    try {
      val available = (mode, factory(desired)) match {
        case (hdfs: Hdfs, ts: STPS) =>
          // This class has structure we can directly query
          BTimePathedSource.satisfiableHdfs(hdfs, desired, factory.asInstanceOf[DateRange => STPS])
        case (_, source) if STry(source.validateTaps(mode)).isSuccess =>
          // If we can validate, there is no need in doing any bisection
          Some(desired)
        case _ => bisectingMinify(mode, desired)(factory)
      }
      available.flatMap { intersect(desired, _) }
        .map(Right(_))
        .getOrElse(Left(List("available: " + available + ", desired: " + desired)))
    } catch { case NonFatal(e) => toTry(e) }

  private def bisectingMinify(mode: Mode, desired: DateRange)(factory: (DateRange) => SSource): Option[DateRange] = {
    def isGood(end: Long): Boolean = STry(factory(DateRange(desired.start, RichDate(end))).validateTaps(mode)).isSuccess
    val DateRange(start, end) = desired
    if (isGood(start.timestamp)) {
      // The invariant is that low isGood, low < upper, and upper isGood == false
      @annotation.tailrec
      def findEnd(low: Long, upper: Long): Long =
        if (low == (upper - 1L))
          low
        else {
          // mid must be > low because upper >= low + 2
          val mid = low + (upper - low) / 2
          if (isGood(mid))
            findEnd(mid, upper)
          else
            findEnd(low, mid)
        }

      if (isGood(end.timestamp)) Some(desired)
      else Some(DateRange(desired.start, RichDate(findEnd(start.timestamp, end.timestamp))))
    } else {
      // No good data
      None
    }
  }

  /**
   * This uses minify to find the smallest subset we can run.
   * If you don't want this behavior, then use pipeFactoryExact which
   * either produces all the DateRange or the whole job fails.
   */
  def pipeFactory[T](factory: (DateRange) => Mappable[T])(implicit timeOf: TimeExtractor[T]): PipeFactory[T] =
    optionMappedPipeFactory(factory)(t => Some(t))

  /**
   * Like pipeFactory, but allows the output of the factory to be mapped.
   *
   * Useful when using TextLine, for example, where the lines need to be
   * parsed before you can extract the timestamps.
   */
  def mappedPipeFactory[T, U](factory: (DateRange) => Mappable[T])(fn: T => U)(implicit timeOf: TimeExtractor[U]): PipeFactory[U] =
    optionMappedPipeFactory(factory)(t => Some(fn(t)))

  /**
   * Like pipeFactory, but allows the output of the factory to be mapped to an optional value.
   *
   * Useful when using TextLine, for example, where the lines need to be
   * parsed before you can extract the timestamps.
   */
  def optionMappedPipeFactory[T, U](factory: (DateRange) => Mappable[T])(fn: T => Option[U])(implicit timeOf: TimeExtractor[U]): PipeFactory[U] =
    StateWithError[(Interval[Timestamp], Mode), List[FailureReason], FlowToPipe[U]] {
      (timeMode: (Interval[Timestamp], Mode)) =>
        {
          val (timeSpan, mode) = timeMode

          toDateRange(timeSpan).right.flatMap { dr =>
            minify(mode, dr)(factory)
              .right.map { newDr =>
                val newIntr = newDr.as[Interval[Timestamp]]
                val mappable = factory(newDr)
                ((newIntr, mode), Reader { (fdM: (FlowDef, Mode)) =>
                  TypedPipe.from(mappable)
                    .flatMap { t =>
                      fn(t).flatMap { mapped =>
                        val time = Timestamp(timeOf(mapped))
                        if (newIntr(time)) Some((time, mapped)) else None
                      }
                    }
                })
              }
          }
        }
    }

  def pipeFactoryExact[T](factory: (DateRange) => Mappable[T])(implicit timeOf: TimeExtractor[T]): PipeFactory[T] =
    StateWithError[(Interval[Timestamp], Mode), List[FailureReason], FlowToPipe[T]] {
      (timeMode: (Interval[Timestamp], Mode)) =>
        {
          val (timeSpan, mode) = timeMode

          toDateRange(timeSpan).right.map { dr =>
            val mappable = factory(dr)
            ((timeSpan, mode), Reader { (fdM: (FlowDef, Mode)) =>
              mappable.validateTaps(fdM._2) //This can throw, but that is what this caller wants
              TypedPipe.from(mappable)
                .flatMap { t =>
                  val time = Timestamp(timeOf(t))
                  if (timeSpan(time)) Some((time, t)) else None
                }
            })
          }
        }
    }

  def sourceFromMappable[T: TimeExtractor: Manifest](
    factory: (DateRange) => Mappable[T]): Producer[Scalding, T] =
    Producer.source[Scalding, T](pipeFactory(factory))

  def toDateRange(timeSpan: Interval[Timestamp]): Try[DateRange] =
    timeSpan.as[Option[DateRange]]
      .map { Right(_) }
      .getOrElse(Left(List("only finite time ranges are supported by scalding: " + timeSpan.toString)))

  /**
   * This makes sure that the output FlowToPipe[T] produces a TypedPipe[T] with only
   * times in the given time interval.
   */
  def limitTimes[T](range: Interval[Timestamp], in: FlowToPipe[T]): FlowToPipe[T] =
    in.map { pipe => pipe.filter { case (time, _) => range(time) } }

  private[scalding] def joinFP[T, U](left: FlowToPipe[T], right: FlowToPipe[U]): FlowProducer[(TimedPipe[T], TimedPipe[U])] =
    for {
      t <- left
      u <- right
    } yield ((t, u))

  def merge[T](left: FlowToPipe[T], right: FlowToPipe[T]): FlowToPipe[T] =
    joinFP(left, right).map { case (l, r) => (l ++ r) }

  /**
   * This does the AlsoProducer logic of making `ensure` a part of the
   * flow, but not this output.
   */
  def also[L, R](ensure: FlowToPipe[L], result: FlowToPipe[R]): FlowToPipe[R] =
    joinFP(ensure, result).map { case (_, r) => r }

  /**
   * Memoize the inner reader
   *  This is not a performance optimization, but a correctness one applicable
   *  to some cases (namely any function that mutates the FlowDef or does IO).
   *  Though we are working in a referentially transparent manner, the application
   *  of the function inside the PipeFactory (the Reader) mutates the FlowDef.
   *  For a fixed PipeFactory, we only want to mutate a given FlowDef once.
   *  If we memoize with this function, it guarantees that the PipeFactory
   *  is idempotent.
   */
  def memoize[T](pf: PipeFactory[T]): PipeFactory[T] = {
    val memo = new Memo[T]
    pf.map { rdr =>
      Reader({ i => memo.getOrElseUpdate(i, rdr) })
    }
  }

  private def getOrElse[T <: AnyRef: ClassTag](options: Map[String, Options],
    names: List[String],
    producer: Producer[Scalding, _], default: => T): T =
    Options.getFirst[T](options, names) match {
      case None =>
        logger.debug(
          s"Producer (${producer.getClass.getName}): Using default setting $default")
        default
      case Some((id, opt)) =>
        logger.info(
          s"Producer (${producer.getClass.getName}) Using $opt found via NamedProducer ${'"'}$id${'"'}")
        opt
    }

  /**
   * Return a PipeFactory that can cover as much as possible of the time range requested,
   * but the output state gives the actual, non-empty, interval that can be produced
   */
  private def buildFlow[T](options: Map[String, Options],
    producer: Producer[Scalding, T],
    fanOuts: Set[Producer[Scalding, _]],
    dependants: Dependants[Scalding],
    built: Map[Producer[Scalding, _], PipeFactory[_]],
    forceFanOut: Boolean = false): (PipeFactory[T], Map[Producer[Scalding, _], PipeFactory[_]]) = {

    val names = dependants.namesOf(producer).map(_.id)

    def recurse[U](p: Producer[Scalding, U],
      built: Map[Producer[Scalding, _], PipeFactory[_]] = built,
      forceFanOut: Boolean = forceFanOut): (PipeFactory[U], Map[Producer[Scalding, _], PipeFactory[_]]) = {
      buildFlow(options, p, fanOuts, dependants, built, forceFanOut)
    }

    // This is used to join in the StateWithError monad that we use to plan
    implicit val modeSemigroup: Semigroup[Mode] = new Semigroup[Mode] {
      def plus(left: Mode, right: Mode) = {
        assert(left == right, "Mode cannot change during planning")
        left
      }
    }

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
      if (forceFanOut || fanOuts(producer))
        p.map { flowP =>
          flowP.map { _.fork }
        }
      else
        p

    built.get(producer) match {
      case Some(pf) => (pf.asInstanceOf[PipeFactory[T]], built)
      case None =>
        val (pf, m) = producer match {
          case Source(src) => {
            val shards = getOrElse(options, names, producer, FlatMapShards.default).count
            val srcPf = if (shards <= 1)
              src
            else
              src.mapPipe(_.shard(shards))

            (srcPf, built)
          }
          case IdentityKeyedProducer(producer) =>
            recurse(producer)
          case NamedProducer(producer, newId) =>
            recurse(producer)
          case summer @ Summer(producer, store, semigroup) =>
            /*
             * The store may already have materialized values, so we don't need the whole
             * input history, but to produce NEW batches, we may need some input.
             * So, we pass the full PipeFactory to to the store so it can request only
             * the time ranges that it needs.
             */
            val shouldForkProducer = InternalService.storeIsJoined(dependants, store)
            val (in, m) = recurse(producer, forceFanOut = shouldForkProducer)
            val commutativity = getCommutativity(names, options, summer)
            val storeReducers = getOrElse(options, names, producer, Reducers.default).count
            logger.info("Store {} using {} reducers (-1 means unset)", store, storeReducers)
            (store.merge(in, semigroup, commutativity, storeReducers), m)
          case LeftJoinedProducer(left, service: ExternalService[_, _]) =>
            /**
             * There is no point loading more from the left than the service can
             * join with, so we pass in the left PipeFactory so that the service
             * can compute how wuch it can actually handle and only load that much
             */
            val (pf, m) = recurse(left)
            (service.lookup(pf), m)
          case ljp @ LeftJoinedProducer(left, StoreService(store)) if InternalService.storeDoesNotDependOnJoin(dependants, ljp, store) =>
            /*
             * This is the simplest case of joining against a store. Here we just need the input to
             * the store and call LookupJoin
             * We use the go method to put the types correctly that scala misses in matching
             */
            def go[K, U, V](left: Producer[Scalding, (K, U)], bstore: BatchedStore[K, V]) = {
              implicit val keyOrdering = bstore.ordering
              val Summer(storeLog, _, sg) = InternalService.getSummer[K, V](dependants, bstore)
                .getOrElse(
                  sys.error("join %s is against store not in the entire job's Dag".format(ljp)))
              val (leftPf, m1) = recurse(left)
              // We have to force the fanOut on the storeLog because this kind of fanout
              // due to joining is not visible in the Dependants dag
              val (logPf, m2) = recurse(storeLog, built = m1, forceFanOut = true)
              // We have to combine the last snapshot on disk with the deltas:
              val allDeltas: PipeFactory[(K, V)] = bstore.readDeltaLog(logPf)
              val reducers = getOrElse(options, names, ljp, Reducers.default).count
              val res = for {
                leftAndDelta <- leftPf.join(allDeltas)
                joined = InternalService.doIndependentJoin[K, U, V](leftAndDelta._1, leftAndDelta._2, sg, Some(reducers))
                // read the latest state, which is the (time interval, mode)
                maxAvailable <- StateWithError.getState
              } yield Scalding.limitTimes(maxAvailable._1, joined)
              (res, m2)
            }
            go(left, store)

          case ljp @ LeftJoinedProducer(left, StoreService(store)) if InternalService.isValidLoopJoin(dependants, left, store) =>
            def go[K, V, U](incoming: Producer[Scalding, (K, V)], bs: BatchedStore[K, U]) = {
              val (flatMapFn, othersOpt) = InternalService.getLoopInputs(dependants, incoming, bs)
              val (leftPf, m1) = recurse(incoming)

              val (deltaLogOpt, m2) = othersOpt match {
                case Some(o) =>
                  val (merges, m2) = recurse(o, built = m1)
                  (Some(bs.readDeltaLog(merges)), m2)
                case None =>
                  (None, m1)
              }
              val reducers = getOrElse(options, names, ljp, Reducers.default).count
              implicit val keyOrdering = bs.ordering
              val Summer(storeLog, _, sg) = InternalService.getSummer[K, U](dependants, bs)
                .getOrElse(
                  sys.error("join %s is against store not in the entire job's Dag".format(ljp)))
              implicit val semigroup: Semigroup[U] = sg
              logger.info("Service {} using {} reducers (-1 means unset)", ljp, reducers)

              val res: PipeFactory[(K, (V, Option[U]))] = for {
                // Handle the Option[Producer] return value from getLoopInputs properly.
                // If there was no producer returned, pass an empty TypedPipe to the join for that part.
                flowToPipe <- deltaLogOpt.map { del =>
                  leftPf.join(del).map {
                    case (ftpA, ftpB) =>
                      Scalding.joinFP(ftpA, ftpB) // extra producer for store, join the two FlowToPipes
                  }
                }.getOrElse(leftPf.map { p => p.map((_, TypedPipe.empty)) }) // no extra producer for store
                servOut = flowToPipe.map {
                  case (lpipe, dpipe) =>
                    InternalService.loopJoin[Timestamp, K, V, U](lpipe, dpipe, flatMapFn, Some(reducers))
                }
                // servOut is both the store output and the join output
                plannedStore = servOut.map(_._1)
              } yield plannedStore

              (res, m2)
            }
            go(left, store)
          case ljp @ LeftJoinedProducer(left, StoreService(store)) =>
            sys.error("Unsupported Join against store: not a valid loop join. If the store depends on join output, only the values can change (filterValues, mapValues, flatMapValues).")
          case WrittenProducer(producer, sink) =>
            val (pf, m) = recurse(producer)
            (sink.write(pf), m)
          case OptionMappedProducer(producer, op) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = recurse(producer)
            (fmp.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap {
                  case (time, item) =>
                    op(item).map((time, _))
                }
              }
            }, m)
          case ValueFlatMappedProducer(producer, op) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = recurse(producer)
            (fmp.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap {
                  case (time, (k, v)) =>
                    op(v).map { u => (time, (k, u)) }
                }
              }
            }, m)
          case kfm @ KeyFlatMappedProducer(producer, op) =>
            val (fmp, m) = recurse(producer)
            /**
             * If the following is true, it is safe to put a mapside reduce before this node:
             * 1) there is only one downstream output, which is a Summer
             * 2) there are only NoOp Producers between this node and the Summer
             */
            val downStream = dependants.transitiveDependantsTillOutput(kfm)
            val maybeMerged = downStream.collect { case t: TailProducer[_, _] => t } match {
              case List(sAny: Summer[_, _, _]) =>
                val s = sAny.asInstanceOf[Summer[Scalding, Any, Any]]
                if (downStream.forall(d => Producer.isNoOp(d) || d == s)) {
                  //only downstream are no-ops and the summer GO!
                  getCommutativity(names, options, s) match {
                    case Commutative =>
                      logger.info("enabling flatMapKeys mapside caching")
                      s.store.partialMerge(fmp, s.semigroup, Commutative)
                    case NonCommutative =>
                      logger.info("not enabling flatMapKeys mapside caching, due to non-commutativity")
                      fmp
                  }
                } else
                  fmp
              case _ => fmp
            }

            // Map in two monads here, first state then reader
            (maybeMerged.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap {
                  case (time, (k, v)) =>
                    op(k).map { newK => (time, (newK, v)) }
                }
              }
            }, m)
          case FlatMappedProducer(producer, op) =>
            // Map in two monads here, first state then reader
            val shards = getOrElse(options, names, producer, FlatMapShards.default).count
            val (fmp, m) = recurse(producer)
            val fmpSharded = if (shards < 1)
              fmp
            else
              fmp.mapPipe(_.shard(shards))

            (fmpSharded.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap {
                  case (time, item) =>
                    op(item).map((time, _))
                }
              }
            }, m)
          case MergedProducer(l, r) => {
            val (pfl, ml) = recurse(l)
            val (pfr, mr) = recurse(r, built = ml)
            val merged = for {
              leftAndRight <- pfl.join(pfr)
              merged = Scalding.merge(leftAndRight._1, leftAndRight._2)
              maxAvailable <- StateWithError.getState // read the latest state, which is the time
            } yield Scalding.limitTimes(maxAvailable._1, merged)
            (merged, mr)
          }
          /**
           * The logic here is identical to a merge except we ignore what comes out of
           * the left side, BUT NOT THE TIME. we can't let the right get ahead of what the
           * left could do to be consistent with the rest of this code.
           */
          case AlsoProducer(l, r) => {
            val (pfl, ml) = recurse(l)
            val (pfr, mr) = recurse(r, built = ml)
            val onlyRight = for {
              leftAndRight <- pfl.join(pfr)
              justRight = Scalding.also(leftAndRight._1, leftAndRight._2)
              maxAvailable <- StateWithError.getState // read the latest state, which is the time
            } yield Scalding.limitTimes(maxAvailable._1, justRight)
            (onlyRight, mr)
          }
        }
        // Make sure that we end any chains of nodes at fanned out nodes:
        val res = memoize(forceNode(pf))
        (res.asInstanceOf[PipeFactory[T]], m + (producer -> res))
    }
  }

  private def planProducer[T](options: Map[String, Options], prod: Producer[Scalding, T]): PipeFactory[T] = {
    val dep = Dependants(prod)
    val fanOutSet =
      dep.nodes
        .filter(dep.fanOut(_).exists(_ > 1)).toSet
    buildFlow(options, prod, fanOutSet, dep, Map.empty)._1
  }

  def plan[T](options: Map[String, Options], prod: TailProducer[Scalding, T]): PipeFactory[T] = {
    planProducer(options, prod)
  }

  /**
   * Use this method to interop with existing scalding code
   * Note this may return a smaller DateRange than you ask for
   * If you need an exact DateRange see toPipeExact.
   */
  def toPipe[T](dr: DateRange,
    prod: Producer[Scalding, T],
    opts: Map[String, Options] = Map.empty)(implicit fd: FlowDef, mode: Mode): Try[(DateRange, TypedPipe[(Timestamp, T)])] = {
    val ts = dr.as[Interval[Timestamp]]
    val pf = planProducer(opts, prod)
    toPipe(ts, fd, mode, pf).right.map {
      case (ts, pipe) =>
        (ts.as[Option[DateRange]].get, pipe)
    }
  }

  /**
   * Use this method to interop with existing scalding code that expects
   * to schedule an exact DateRange or fail.
   */
  def toPipeExact[T](dr: DateRange,
    prod: Producer[Scalding, T],
    opts: Map[String, Options] = Map.empty)(implicit fd: FlowDef, mode: Mode): Try[TypedPipe[(Timestamp, T)]] = {
    val ts = dr.as[Interval[Timestamp]]
    val pf = planProducer(opts, prod)
    toPipeExact(ts, fd, mode, pf)
  }

  def toPipe[T](timeSpan: Interval[Timestamp],
    flowDef: FlowDef,
    mode: Mode,
    pf: PipeFactory[T]): Try[(Interval[Timestamp], TimedPipe[T])] = {
    logger.info("topipe Planning on interval: {}", timeSpan)
    pf((timeSpan, mode))
      .right
      .map { case (((ts, m), flowDefMutator)) => (ts, flowDefMutator((flowDef, m))) }
  }

  def toPipeExact[T](timeSpan: Interval[Timestamp],
    flowDef: FlowDef,
    mode: Mode,
    pf: PipeFactory[T]): Try[TimedPipe[T]] = {
    logger.info("Planning on interval: {}", timeSpan.as[Option[DateRange]])
    pf((timeSpan, mode))
      .right
      .flatMap {
        case (((ts, m), flowDefMutator)) =>
          if (ts != timeSpan) Left(List("Could not load all of %s, only %s".format(ts, timeSpan)))
          else Right(flowDefMutator((flowDef, m)))
      }
  }
}

// Jank to get around serialization issues
class Memo[T] extends java.io.Serializable {
  @transient private val mmap = scala.collection.mutable.Map[(FlowDef, Mode), TimedPipe[T]]()
  def getOrElseUpdate(in: (FlowDef, Mode), rdr: Reader[(FlowDef, Mode), TimedPipe[T]]): TimedPipe[T] =
    mmap.getOrElseUpdate(in, rdr(in))
}

/**
 * Use this option to write the logical graph that cascading
 * produces before Map/Reduce planning.
 * Use the job name as the key
 */
case class WriteDot(filename: String)

/**
 * Use this option to write map/reduce graph
 * that cascading produces
 * Use the job name as the key
 */
case class WriteStepsDot(filename: String)

class Scalding(
  val jobName: String,
  @transient val options: Map[String, Options],
  @transient transformConfig: Config => Config,
  @transient passedRegistrars: List[IKryoRegistrar])
    extends Platform[Scalding] with java.io.Serializable {

  type Source[T] = PipeFactory[T]
  type Store[K, V] = scalding.Store[K, V]
  type Sink[T] = scalding.Sink[T]
  type Service[K, V] = scalding.Service[K, V]
  type Plan[T] = PipeFactory[T]

  def plan[T](prod: TailProducer[Scalding, T]): PipeFactory[T] =
    Scalding.plan(options, prod)

  def withRegistrars(newRegs: List[IKryoRegistrar]) =
    new Scalding(jobName, options, transformConfig, newRegs ++ passedRegistrars)

  def withConfigUpdater(fn: Config => Config) =
    new Scalding(jobName, options, transformConfig.andThen(fn), passedRegistrars)

  def configProvider(hConf: Configuration): Config = {
    import com.twitter.scalding._
    import com.twitter.chill.config.ScalaMapConfig
    val conf = Config.hadoopWithDefaults(hConf)

    if (passedRegistrars.isEmpty) {
      conf.setSerialization(Right(classOf[serialization.KryoHadoop]))
    } else {
      val kryoReg = new IterableRegistrar(passedRegistrars)
      val initKryo = conf.getKryo match {
        case None =>
          new serialization.KryoHadoop(ScalaMapConfig(conf.toMap))
        case Some(kryo) => kryo
      }

      conf
        .setSerialization(
          Left((classOf[serialization.KryoHadoop], initKryo.withRegistrar(kryoReg))), Nil)
    }
  }

  final def buildConfig(hConf: Configuration): Config = {
    val config = transformConfig(configProvider(hConf))

    // Store the options used:
    val postConfig = config.+("summingbird.options" -> options.toString)
      .+("summingbird.jobname" -> jobName)
      .+("summingbird.submitted.timestamp" -> System.currentTimeMillis.toString)

    postConfig.toMap.foreach { case (k, v) => hConf.set(k, v) }
    postConfig
  }

  // This is a side-effect-free computation that is called by run
  def toFlow(config: Config, timeSpan: Interval[Timestamp], mode: Mode, pf: PipeFactory[_]): Try[(Interval[Timestamp], Option[Flow[_]])] = {
    val flowDef = new FlowDef
    flowDef.setName(jobName)
    Scalding.toPipe(timeSpan, flowDef, mode, pf)
      .right
      .flatMap {
        case (ts, pipe) =>
          // Now we have a populated flowDef, time to let Cascading do it's thing:
          try {
            if (flowDef.getSinks.isEmpty) {
              Right((ts, None))
            } else {
              Right((ts, Some(mode.newFlowConnector(config).connect(flowDef))))
            }
          } catch {
            case NonFatal(e) => toTry(e)
          }
      }
  }

  def run(state: WaitingState[Interval[Timestamp]],
    mode: Mode,
    pf: TailProducer[Scalding, Any]): WaitingState[Interval[Timestamp]] =
    run(state, mode, plan(pf))

  def run(state: WaitingState[Interval[Timestamp]],
    mode: Mode,
    pf: PipeFactory[Any]): WaitingState[Interval[Timestamp]] = run(state, mode, pf, (f: Flow[_]) => Unit)

  def run(state: WaitingState[Interval[Timestamp]],
    mode: Mode,
    pf: PipeFactory[Any],
    mutate: Flow[_] => Unit): WaitingState[Interval[Timestamp]] = {

    val config = mode match {
      case Hdfs(_, conf) =>
        buildConfig(conf)
      case HadoopTest(conf, _) =>
        buildConfig(conf)

      case _ => Config.empty
    }

    val prepareState = state.begin
    toFlow(config, prepareState.requested, mode, pf) match {
      case Left(errs) =>
        prepareState.fail(FlowPlanException(errs))
      case Right((ts, flowOpt)) =>
        flowOpt.foreach(flow => mutate(flow))
        prepareState.willAccept(ts) match {
          case Right(runningState) =>
            try {
              flowOpt match {
                case None =>
                  Scalding.logger.warn("No Sinks were planned into flows. Waiting state is probably out of sync with stores. Proceeding with NO-OP.")
                  runningState.succeed
                case Some(flow) =>
                  options.get(jobName).foreach { jopt =>
                    jopt.get[WriteDot].foreach { o => flow.writeDOT(o.filename) }
                    jopt.get[WriteStepsDot].foreach { o => flow.writeStepsDOT(o.filename) }
                  }
                  flow.complete
                  if (flow.getFlowStats.isSuccessful)
                    runningState.succeed
                  else
                    throw new Exception("Flow did not complete.")
              }
            } catch {
              case NonFatal(e) => runningState.fail(e)
            }
          case Left(waitingState) => waitingState
        }
    }
  }
}
