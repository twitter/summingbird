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

import com.twitter.algebird.Semigroup
import com.twitter.scalding.TypedPipe
import com.twitter.summingbird._
import com.twitter.summingbird.option._
import com.twitter.summingbird.scalding.batch.BatchedStore

import com.twitter.algebird.monad.Reader
import com.twitter.scalding.{ Mode, TypedPipe }
import cascading.flow.FlowDef

sealed trait Service[K, +V] extends java.io.Serializable

/**
 * This represents a service that is *external* to the current job.
 * This does not include joins for data that is generated in the same
 * Producer graph
 */
trait ExternalService[K, +V] extends Service[K, V] {
  // A static, or write-once service can  potentially optimize this without writing the (K, V) stream out
  def lookup[W](getKeys: PipeFactory[(K, W)]): PipeFactory[(K, (W, Option[V]))]
}

/**
 * This represents a join against data that is materialized by a store
 * in the current job
 */
sealed trait InternalService[K, +V] extends Service[K, V]
case class StoreService[K, V](store: BatchedStore[K, V]) extends InternalService[K, V] with Store[K, V] {

  def merge(delta: PipeFactory[(K, V)],
    sg: Semigroup[V],
    commutativity: Commutativity,
    reducers: Int): PipeFactory[(K, (Option[V], V))] = {
    store.merge(delta, sg, commutativity, reducers)
  }
}

/**
 * Here are some methods that are useful in planning the execution of Internal Services
 */
private[scalding] object InternalService {

  def storeDoesNotDependOnJoin[K, V](dag: Dependants[Scalding],
    joinProducer: Producer[Scalding, Any],
    store: BatchedStore[K, V]): Boolean = {

    // in all of the graph, find a summer node Summer(_, thatStore, _) where thatStore == store
    // and see if this summer depends on the given leftJoin
    !dag.nodes.exists { n =>
      n match {
        case summer @ Summer(_, StoreService(thatStore), _) if thatStore == store =>
          Producer.transitiveDependenciesOf(summer)
            .collectFirst { case ljp @ LeftJoinedProducer(l, s) if ljp == joinProducer => () }
            .isDefined
        case _ => false
      }
    }
  }

  def isValidLoopJoin[K, V](dag: Dependants[Scalding], left: Producer[Scalding, Any],
    store: BatchedStore[K, V]): Boolean = {
    /*
       * this needs to check if:
       * 1) There is only one dependant path from join to store.
       * 2) After the join, there are only flatMapValues (later we can handle merges as well)
       */
    val summerToStore =
      getSummer[K, V](dag, store).getOrElse(sys.error("Could not find the Summer for store."))

    val depsOfSummer: List[Producer[Scalding, Any]] = Producer.transitiveDependenciesOf(summerToStore)

    def recurse(p: Producer[Scalding, Any]): Boolean = {
      p match {
        case ValueFlatMappedProducer(lprod, _) =>
          recurse(lprod)
        case IdentityKeyedProducer(prod) =>
          recurse(prod)
        case NamedProducer(prod, _) =>
          recurse(prod)
        case LeftJoinedProducer(prod, joined) if prod == left =>
          true // done, valid dag
        case _ =>
          false // hit a node that is not one of the allowed ones, invalid loop
      }
    }
    recurse(depsOfSummer.head)
  }

  def storeIsJoined[K, V](dag: Dependants[Scalding], store: Store[K, V]): Boolean =
    dag.nodes.exists {
      case LeftJoinedProducer(l, StoreService(s)) => s == store
      case _ => false
    }

  // Get the summer that sums into the given store
  def getSummer[K, V](dag: Dependants[Scalding],
    store: BatchedStore[K, V]): Option[Summer[Scalding, K, V]] = {
    // what to do if there is more than one summer here?
    dag.nodes.collectFirst {
      case summer @ Summer(p, StoreService(thatStore), _) if (thatStore == store) =>
        summer.asInstanceOf[Summer[Scalding, K, V]]
    }
  }

  /**
   * Just wire in LookupJoin here. This method assumes that
   * the FlowToPipe is already on the matching time, so we don't
   * need to worry about that here.
   */
  def doIndependentJoin[K: Ordering, U, V](input: FlowToPipe[(K, U)],
    toJoin: FlowToPipe[(K, V)],
    sg: Semigroup[V],
    reducers: Option[Int]): FlowToPipe[(K, (U, Option[V]))] =

    Reader[FlowInput, KeyValuePipe[K, (U, Option[V])]] { (flowMode: (FlowDef, Mode)) =>
      val left = input(flowMode)
      val right = toJoin(flowMode)
      LookupJoin.rightSumming(left, right, reducers)(implicitly, implicitly, sg)
    }

  /**
   * This looks into the dag, and finds the mapping function into the store
   * and a producer of any merged input into the store
   */
  def getLoopInputs[K, U, V](dag: Dependants[Scalding],
    left: Producer[Scalding, (K, U)],
    store: BatchedStore[K, V]): ((((U, Option[V])) => TraversableOnce[V]), Option[Producer[Scalding, (K, V)]]) = {

    val Summer(summerProd, _, _) = getSummer[K, V](dag, store).getOrElse(sys.error("Could not find the Summer for store."))

    type ValueFlatMapFn = ((U, Option[V])) => TraversableOnce[V]

    val res: Option[(ValueFlatMapFn, Option[Producer[Scalding, (K, V)]])] = {
      /*
       * Return the (maybe composed from multiple fns) value[Flat]Map function,
       * and two boolean flags: 1) seen at least one ValueFlatMap and
       * 2) processed a valid dag, i.e. not seen any producers between the join and store besides
       * ValueFlatMappedProducer (at least one), IdentityKeyedProducer and NamedProducer
       */
      def recurse(p: Producer[Scalding, Any],
        cummulativeFn: Option[(Any) => TraversableOnce[Any]]): Option[(Any) => TraversableOnce[Any]] =
        p match {
          case ValueFlatMappedProducer(prod, fn) =>
            cummulativeFn match {
              case Some(cfn) => {
                val newFn = (e: Any) => fn(e).flatMap { r => cfn(r) }
                recurse(prod, Some(newFn))
              }
              case None => recurse(prod, Some(fn))

            }
          case IdentityKeyedProducer(prod) =>
            recurse(prod, cummulativeFn)
          case NamedProducer(prod, _) =>
            recurse(prod, cummulativeFn)
          case LeftJoinedProducer(prod, joined) if prod == left =>
            cummulativeFn
        }

      val fn = recurse(summerProd, None)
      fn.map { f => (f.asInstanceOf[ValueFlatMapFn], None) }
    }

    res.getOrElse(sys.error("Could not find correct loop inputs for leftJoin-store loop. Check the job DAG for validity."))
  }

  /**
   * This is for the case where the left items come in, then we sum the second mergeLog.
   *
   * @param left TypedPipe of producer of input to the join
   * @param mergeLog TypedPipe of merges to the store
   * @param valueExpansion a function on the values coming out of the join
   * @param reduers an option number of reducers to use for the join
   *
   * This function performs the loop join by sorting the input by time and then calling scanLeft to merge the two TypedPipes.
   * The result is a join stream and the output stream of the store.
   */
  def loopJoin[T: Ordering, K: Ordering, V, U: Semigroup](left: TypedPipe[(T, (K, V))],
    mergeLog: TypedPipe[(T, (K, U))],
    valueExpansion: ((V, Option[U])) => TraversableOnce[U],
    reducers: Option[Int]): (TypedPipe[(T, (K, (V, Option[U])))], TypedPipe[(T, (K, (Option[U], U)))]) = {

    def sum(opt: Option[U], u: U): U = if (opt.isDefined) Semigroup.plus(opt.get, u) else u

    /**
     * Make sure lookups happen before writes to the store IF the timestamp
     * is the same (reads and writes at the same time are ordered so that reads
     * happen first since they are earlier in the graph).
     * This weird E trick is because the inferred type below is
     * Product with Serializable with Either[V, U]
     */
    implicit def lookupFirst[E <: Either[V, U]]: Ordering[E] = Ordering.by {
      case Left(_) => 0
      case Right(_) => 1
    }

    val bothPipes = (left.map { case (t, (k, v)) => (k, (t, Left(v))) } ++
      mergeLog.map { case (t, (k, u)) => (k, (t, Right(u))) })
      .group
      .withReducers(reducers.getOrElse(-1)) // jank, but scalding needs a way to maybe set reducers
      .sorted
      .scanLeft((Option.empty[(T, (V, Option[U]))], Option.empty[(T, (Option[U], U))])) {
        case ((_, None), (time, Left(v))) =>
          /*
           * This is a lookup, but there is no value for this key
           */
          val joinResult = Some((time, (v, None)))
          val sumResult = Semigroup.sumOption(valueExpansion((v, None))).map(u => (time, (None, u)))
          (joinResult, sumResult)
        case ((_, Some((_, (optu, u)))), (time, Left(v))) =>
          /*
           * This is a lookup, and there is an existing value
           */
          val currentU = Some(sum(optu, u)) // isn't u already a sum and optu prev value?
          val joinResult = Some((time, (v, currentU)))
          val sumResult = Semigroup.sumOption(valueExpansion((v, currentU))).map(u => (time, (currentU, u)))
          (joinResult, sumResult)
        case ((_, None), (time, Right(u))) =>
          /*
           * This is merging in new data into the store not coming in from the service
           * (either from the store history or from a merge after the leftJoin, but
           * There was previously no data.
           */
          val joinResult = None
          val sumResult = Some((time, (None, u)))
          (joinResult, sumResult)
        case ((_, Some((_, (optu, oldu)))), (time, Right(u))) =>
          /*
           * This is the case where we are updating a non-empty key. This should
           * only be triggered by a merged data-stream after the join since
           * store initialization
           */
          val joinResult = None
          val currentU = Some(sum(optu, oldu))
          val sumResult = Some((time, (currentU, u)))
          (joinResult, sumResult)
      }
      .toTypedPipe
      // We forceToDisk because we can't do two writes from one TypedPipe
      .forceToDisk

    val leftOut = bothPipes.collect {
      case (k, (Some((t, vu)), _)) =>
        (t, (k, vu))
    }
    val rightOut = bothPipes.collect {
      case (k, (_, Some((t, optuu)))) =>
        (t, (k, optuu))
    }
    (leftOut, rightOut)
  }
}
