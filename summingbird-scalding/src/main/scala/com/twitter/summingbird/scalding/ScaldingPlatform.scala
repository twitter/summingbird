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
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.Injection
import com.twitter.scalding.{ Tool => STool, Source => SSource, TimePathedSource => STPS, _}
import com.twitter.summingbird._
import com.twitter.summingbird.scalding.option.{ FlatMapShards, Reducers }
import com.twitter.summingbird.batch._
import com.twitter.chill.IKryoRegistrar
import com.twitter.summingbird.chill._
import com.twitter.summingbird.option._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.serializer.{Serialization => HSerialization}
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.GenericOptionsParser
import java.util.{ HashMap => JHashMap, Map => JMap, TimeZone }
import cascading.flow.{FlowDef, Flow}
import com.twitter.scalding.Mode

import scala.util.{ Success, Failure }
import scala.util.control.Exception.allCatch

import org.slf4j.LoggerFactory

object Scalding {
  @transient private val logger = LoggerFactory.getLogger(classOf[Scalding])


  def apply(jobName: String, options: Map[String, Options] = Map.empty) = {
    new Scalding(jobName, options, identity, List())
  }

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
      Success(DateRange(RichDate(low), RichDate(high)))
    case _ => Failure(new RuntimeException("Unbounded interval!"))
  }


  def emptyFlowProducer[T]: FlowProducer[TypedPipe[T]] =
    Reader({ implicit fdm: (FlowDef, Mode) => TypedPipe.empty })

  def getCommutativity(names: List[String],
    options: Map[String, Options],
    s: Summer[Scalding, _, _]): Commutativity = {

    val commutativity = getOrElse(options, names, s, {
      logger.warn("Store: {} has no commutativity setting. Assuming {}",
          names, MonoidIsCommutative.default)
      MonoidIsCommutative.default
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
          case (hdfs: Hdfs, ts: STPS) =>
            TimePathedSource.satisfiableHdfs(hdfs, desired, factory.asInstanceOf[DateRange => STPS])
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
                TypedPipe.from(mappable)(fdM._1, fdM._2)
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
            TypedPipe.from(mappable)(fdM._1, fdM._2)
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

  def limitTimes[T](range: Interval[Time], in: FlowToPipe[T]): FlowToPipe[T] =
    in.map { pipe => pipe.filter { case (time, _) => range(time) } }

  def merge[T](left: FlowToPipe[T], right: FlowToPipe[T]): FlowToPipe[T] =
    for { l <- left; r <- right } yield (l ++ r)

  /**
   * This does the AlsoProducer logic of making `ensure` a part of the
   * flow, but not this output.
   */
  def also[L,R](ensure: FlowToPipe[L], result: FlowToPipe[R]): FlowToPipe[R] =
    for { _ <- ensure; r <- result } yield r

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


  private def getOrElse[T <: AnyRef : Manifest](options: Map[String, Options], names: List[String], producer: Producer[Scalding, _], default: => T): T = {
    val maybePair = (for {
      id <- names :+ "DEFAULT"
      innerOpts <- options.get(id)
      option <- innerOpts.get[T]
    } yield (id, option)).headOption

    maybePair match {
      case None =>
          logger.debug("Producer ({}): Using default setting {}", producer.getClass.getName, default)
          default
      case Some((id, opt)) =>
          logger.info("Producer ({}) Using {} found via NamedProducer \"{}\"", Array[AnyRef](producer.getClass.getName, opt, id))
          opt
    }
  }


  /** Return a PipeFactory that can cover as much as possible of the time range requested,
   * but the output state gives the actual, non-empty, interval that can be produced
   */
  private def buildFlow[T](options: Map[String, Options],
    producer: Producer[Scalding, T],
    fanOuts: Set[Producer[Scalding, _]],
    dependants: Dependants[Scalding],
    built: Map[Producer[Scalding, _], PipeFactory[_]]): (PipeFactory[T], Map[Producer[Scalding, _], PipeFactory[_]]) = {
    val names = dependants.namesOf(producer).map(_.id)

    def recurse[U](p: Producer[Scalding, U],
      built: Map[Producer[Scalding, _], PipeFactory[_]] = built):
      (PipeFactory[U], Map[Producer[Scalding, _], PipeFactory[_]]) =
        buildFlow(options, p, fanOuts, dependants, built)

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
          case IdentityKeyedProducer(producer) => recurse(producer)
          case NamedProducer(producer, newId)  => recurse(producer)
          case summer@Summer(producer, store, monoid) =>
            /*
             * The store may already have materialized values, so we don't need the whole
             * input history, but to produce NEW batches, we may need some input.
             * So, we pass the full PipeFactory to to the store so it can request only
             * the time ranges that it needs.
             */
            val (in, m) = recurse(producer)
            val commutativity = getCommutativity(names, options, summer)
            val storeReducers = getOrElse(options, names, producer, Reducers.default).count
            logger.info("Store {} using {} reducers (-1 means unset)", store, storeReducers)
            (store.merge(in, monoid, commutativity, storeReducers), m)
          case LeftJoinedProducer(left, service) =>
            /**
             * There is no point loading more from the left than the service can
             * join with, so we pass in the left PipeFactory so that the service
             * can compute how wuch it can actually handle and only load that much
             */
            val (pf, m) = recurse(left)
            (service.lookup(pf), m)
          case WrittenProducer(producer, sink) =>
            val (pf, m) = recurse(producer)
            (sink.write(pf), m)
          case OptionMappedProducer(producer, op) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = recurse(producer)
            (fmp.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap { case (time, item) =>
                  op(item).map((time, _))
                }
              }
            }, m)
          case kfm@KeyFlatMappedProducer(producer, op) =>
            val (fmp, m) = recurse(producer)
            /**
             * If the following is true, it is safe to put a mapside reduce before this node:
             * 1) there is only one downstream output, which is a Summer
             * 2) there are only NoOp Producers between this node and the Summer
             */
            val downStream = dependants.transitiveDependantsTillOutput(kfm)
            val maybeMerged = downStream.collect { case t: TailProducer[_, _] => t }
              match {
                case List(sAny:Summer[_,_,_]) =>
                  val s = sAny.asInstanceOf[Summer[Scalding, Any, Any]]
                  if(downStream.forall(d => Producer.isNoOp(d) || d == s)) {
                    //only downstream are no-ops and the summer GO!
                    getCommutativity(names, options, s) match {
                      case Commutative =>
                        logger.info("enabling flatMapKeys mapside caching")
                        s.store.partialMerge(fmp, s.monoid, Commutative)
                      case NonCommutative =>
                        logger.info("not enabling flatMapKeys mapside caching, due to non-commutativity")
                        fmp
                    }
                  }
                  else
                    fmp
                case _ => fmp
              }

            // Map in two monads here, first state then reader
            (maybeMerged.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap { case (time, (k, v)) =>
                  op(k).map{newK => (time, (newK, v))}
                }
              }
            }, m)
          case FlatMappedProducer(producer, op) =>
            // Map in two monads here, first state then reader
            val (fmp, m) = recurse(producer)
            (fmp.map { flowP =>
              flowP.map { typedPipe =>
                typedPipe.flatMap { case (time, item) =>
                  op(item).map((time, _))
                }
              }
            }, m)
          case MergedProducer(l, r) => {
            val (pfl, ml) = recurse(l)
            val (pfr, mr) = recurse(r, built = ml)
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
          /** The logic here is identical to a merge except we ignore what comes out of
           * the left side, BUT NOT THE TIME. we can't let the right get ahead of what the
           * left could do to be consistent with the rest of this code.
           */
          case AlsoProducer(l, r) => {
            val (pfl, ml) = recurse(l)
            val (pfr, mr) = recurse(r, built = ml)
            val onlyRight = for {
              // concatenate errors (++) and find the intersection (&&) of times
              leftAndRight <- pfl.join(pfr,
                { (lerr: List[FailureReason], rerr: List[FailureReason]) => lerr ++ rerr },
                { case ((tsl, leftFM), (tsr, _)) => (tsl && tsr, leftFM) })
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

  /** Use this method to interop with existing scalding code
   * Note this may return a smaller DateRange than you ask for
   * If you need an exact DateRange see toPipeExact.
   */
  def toPipe[T](dr: DateRange,
    prod: Producer[Scalding, T],
    opts: Map[String, Options] = Map.empty)(implicit fd: FlowDef, mode: Mode): Try[(DateRange, TypedPipe[(Long, T)])] = {
      val ts = dr.as[Interval[Time]]
      val pf = planProducer(opts, prod)
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
      val pf = planProducer(opts, prod)
      toPipeExact(ts, fd, mode, pf)
    }

  def toPipe[T](timeSpan: Interval[Time],
    flowDef: FlowDef,
    mode: Mode,
    pf: PipeFactory[T]): Try[(Interval[Time], TimedPipe[T])] = {
    logger.info("Planning on interval: {}", timeSpan.as[Option[DateRange]])
    pf((timeSpan, mode))
      .right
      .map { case (((ts, m), flowDefMutator)) => (ts, flowDefMutator((flowDef, m))) }
    }

  def toPipeExact[T](timeSpan: Interval[Time],
    flowDef: FlowDef,
    mode: Mode,
    pf: PipeFactory[T]): Try[TimedPipe[T]] = {
    logger.info("Planning on interval: {}", timeSpan.as[Option[DateRange]])
    pf((timeSpan, mode))
      .right
      .flatMap { case (((ts, m), flowDefMutator)) =>
        if(ts != timeSpan) Left(List("Could not load all of %s, only %s".format(ts, timeSpan)))
        else Right(flowDefMutator((flowDef, m)))
      }
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
  val jobName: String,
  @transient val options: Map[String, Options],
  @transient transformConfig: SummingbirdConfig => SummingbirdConfig,
  @transient passedRegistrars: List[IKryoRegistrar]
  )
    extends Platform[Scalding] {

  type Source[T] = PipeFactory[T]
  type Store[K, V] = ScaldingStore[K, V]
  type Sink[T] = ScaldingSink[T]
  type Service[K, V] = ScaldingService[K, V]
  type Plan[T] = PipeFactory[T]

  def plan[T](prod: TailProducer[Scalding, T]): PipeFactory[T] =
    Scalding.plan(options, prod)

  protected def ioSerializations: List[Class[_ <: HSerialization[_]]] = List(
    classOf[org.apache.hadoop.io.serializer.WritableSerialization],
    classOf[cascading.tuple.hadoop.TupleSerialization],
    classOf[com.twitter.chill.hadoop.KryoSerialization]
  )

  def withRegistrars(newRegs: List[IKryoRegistrar]) =
    new Scalding(jobName, options, transformConfig, newRegs ++ passedRegistrars)

  def withConfigUpdater(fn: SummingbirdConfig => SummingbirdConfig) =
    new Scalding(jobName, options, transformConfig.andThen(fn), passedRegistrars)

  def updateConfig(conf: Configuration) {
    val scaldingConfig = SBChillRegistrar(ScaldingConfig(conf), passedRegistrars)
    Scalding.logger.debug("Serialization config changes:")
    Scalding.logger.debug("Removes: {}", scaldingConfig.removes)
    Scalding.logger.debug("Updates: {}", scaldingConfig.updates)

    // now let the user do her thing:
    val transformedConfig = transformConfig(scaldingConfig)

    Scalding.logger.debug("User+Serialization config changes:")
    Scalding.logger.debug("Removes: {}", transformedConfig.removes)
    Scalding.logger.debug("Updates: {}", transformedConfig.updates)

    transformedConfig.removes.foreach(conf.set(_, null))
    transformedConfig.updates.foreach(kv => conf.set(kv._1, kv._2.toString))
    // Store the options used:
    conf.set("summingbird.options", options.toString)
    conf.set("summingbird.jobname", jobName)
    // legacy name to match scalding
    conf.set("scalding.flow.submitted.timestamp",
          System.currentTimeMillis.toString)
    conf.set("summingbird.submitted.timestamp",
          System.currentTimeMillis.toString)

    def ifUnset(k: String, v: String) { if(null == conf.get(k)) { conf.set(k, v) } }
    // Set the mapside cache size, this is important to not be too small
    ifUnset("cascading.aggregateby.threshold", "100000")
  }

  private def setIoSerializations(c: Configuration): Unit =
      c.set("io.serializations", ioSerializations.map { _.getName }.mkString(","))

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

  def run(state: WaitingState[Interval[Timestamp]],
    mode: Mode,
    pf: TailProducer[Scalding, Any]): WaitingState[Interval[Timestamp]] =
    run(state, mode, plan(pf))

  def run(state: WaitingState[Interval[Timestamp]],
    mode: Mode,
    pf: PipeFactory[Any]): WaitingState[Interval[Timestamp]] = {

    mode match {
      case Hdfs(_, conf) =>
        updateConfig(conf)
        setIoSerializations(conf)
      case _ =>
    }



    val prepareState = state.begin
    val timeSpan = prepareState.requested.mapNonDecreasing(_.milliSinceEpoch)
    toFlow(timeSpan, mode, pf) match {
      case Left(errs) =>
        prepareState.fail(FlowPlanException(errs))
      case Right((ts,flow)) =>
        prepareState.willAccept(ts.mapNonDecreasing(Timestamp(_))) match {
          case Right(runningState) =>
            try {
              options.get(jobName).foreach { jopt =>
                jopt.get[WriteDot].foreach { o => flow.writeDOT(o.filename) }
                jopt.get[WriteStepsDot].foreach { o => flow.writeStepsDOT(o.filename) }
              }
              flow.complete
              if (flow.getFlowStats.isSuccessful)
                runningState.succeed
              else
                runningState.fail(new Exception("Flow did not complete."))
            } catch {
              case (e: Throwable) => {
                runningState.fail(e)
              }
            }
          case Left(waitingState) => waitingState
        }
    }
  }
}
