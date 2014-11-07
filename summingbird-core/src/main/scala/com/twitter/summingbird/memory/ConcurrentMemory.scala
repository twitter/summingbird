/*
 Copyright 2014 Twitter, Inc.

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

package com.twitter.summingbird.memory

import com.twitter.summingbird.graph._

import com.twitter.summingbird.planner.DagOptimizer

import com.twitter.algebird.{ Monoid, Semigroup }
import com.twitter.summingbird._
import com.twitter.summingbird.option.JobId
import scala.collection.mutable.Buffer
import scala.concurrent.{ ExecutionContext, Future }
import java.util.concurrent.{ BlockingQueue, LinkedBlockingQueue, ConcurrentHashMap }

object ConcurrentMemory {
  implicit def toSource[T](traversable: TraversableOnce[T]): Producer[ConcurrentMemory, T] =
    Producer.source[ConcurrentMemory, T](traversable)
}

object ConcurrentMemoryPlan {
  implicit def monoid: Monoid[ConcurrentMemoryPlan] = new Monoid[ConcurrentMemoryPlan] {
    def zero = NullPlan
    def plus(left: ConcurrentMemoryPlan, right: ConcurrentMemoryPlan) = new ConcurrentMemoryPlan {
      def run(implicit ec: ExecutionContext) = left.run.zip(right.run).map(_ => ())
    }
  }
}

trait ConcurrentMemoryPlan {
  def run(implicit ec: ExecutionContext): Future[Unit]
}

object NullPlan extends ConcurrentMemoryPlan {
  def run(implicit ec: ExecutionContext) = Future.successful(())
}

/**
 * This type is what we model the summingbird graphs into.
 * Below, inside PhysicalNode, you will see several implementations
 */
sealed trait PhysicalNode[-I] {
  def push(item: I)(implicit ec: ExecutionContext): Future[Unit]
}

object PhysicalNode {
  case class SourceNode[O](data: TraversableOnce[O],
      next: PhysicalNode[O]) extends ConcurrentMemoryPlan with PhysicalNode[Nothing] {
    def run(implicit ec: ExecutionContext): Future[Unit] =
      Future.sequence(data.map { o => next.push(o) }).map(_ => ())

    def push(item: Nothing)(implicit ec: ExecutionContext) = Future.successful(())
  }
  /**
   * This target is the end of the line for a target that has no dependants.
   */
  case object NullTarget extends PhysicalNode[Any] {
    def push(item: Any)(implicit ec: ExecutionContext) = Future.successful(())
  }
  /**
   * This is a convenience that pushes each item it receieves a sequence of other
   * PhysicalNodes. This is so we don't have to deal with the notion of fan-out
   * on the summingbird graph (multiple consumers from a single node) in each type
   * of node.
   */
  case class FanOut[T](targets: Seq[PhysicalNode[T]]) extends PhysicalNode[T] {
    def push(item: T)(implicit ec: ExecutionContext) =
      Future.sequence(targets.map(_.push(item))).map(_ => ())
  }
  /**
   * This is the PhysicalNode that implements flatMap and sends it on to the next
   * Node.
   */
  case class FlatMap[I, O](fn: I => TraversableOnce[O],
      target: PhysicalNode[O]) extends PhysicalNode[I] {

    def push(item: I)(implicit ec: ExecutionContext) =
      Future.sequence(fn(item).map(target.push(_))).map(_ => ())
  }
  case class Join[K, V, W](service: K => Option[W],
    target: PhysicalNode[(K, (V, Option[W]))])
      extends PhysicalNode[(K, V)] {
    def push(item: (K, V))(implicit ec: ExecutionContext) = {
      val (k, v) = item
      val toPush = (k, (v, service(k)))
      target.push(toPush)
    }
  }
  case class Sum[K, V](store: ConcurrentHashMap[K, V],
    sg: Semigroup[V],
    target: PhysicalNode[(K, (Option[V], V))])
      extends PhysicalNode[(K, V)] {
    def push(item: (K, V))(implicit ec: ExecutionContext) = {
      val (k, v) = item
      @annotation.tailrec
      def go: Option[V] =
        Option(store.get(k)) match {
          case None =>
            if (store.putIfAbsent(k, v) == null) None
            else go
          case s @ Some(oldV) =>
            if (store.replace(k, oldV, sg.plus(oldV, v))) s
            else go
        }
      target.push((k, (go, v)))
    }
  }
  /**
   * This does a write and after that succeeds, pushes
   * onto the next node (which may be the NullTarget)
   */
  case class Writer[T](queue: BlockingQueue[T],
    target: PhysicalNode[T])
      extends PhysicalNode[T] {
    def push(item: T)(implicit ec: ExecutionContext) = for {
      _ <- Future(queue.put(item))
      _ <- target.push(item)
    } yield ()
  }
}

class ConcurrentMemory(implicit jobID: JobId = JobId("default.concurrent.memory.jobId"))
    extends Platform[ConcurrentMemory] with DagOptimizer[ConcurrentMemory] {

  type Source[T] = TraversableOnce[T]
  type Store[K, V] = ConcurrentHashMap[K, V]
  type Sink[T] = BlockingQueue[T]
  type Service[-K, +V] = (K => Option[V])
  type Plan[T] = ConcurrentMemoryPlan

  import PhysicalNode._

  type ProdCons[T] = Prod[Any]

  def counter(group: Group, name: Name): Option[Long] =
    MemoryStatProvider.getCountersForJob(jobID).flatMap { _.get(group.getString + "/" + name.getString).map { _.get } }

  /**
   * This is the main recursive function that plans "that" to
   * PhysicalNode by planning all its dependants first, then
   * planning the that to push into those dependants next
   */
  private def toPhys[T](deps: Dependants[ConcurrentMemory],
    planned0: HMap[ProdCons, PhysicalNode],
    that: Prod[Any]): (HMap[ProdCons, PhysicalNode], PhysicalNode[T]) =
    planned0.get(that) match {
      case Some(s) => (planned0, s)
      case None =>
        def maybeFanout[U]: (HMap[ProdCons, PhysicalNode], PhysicalNode[U]) =
          deps.dependantsAfterMerge(that) match {
            case Nil => (planned0, NullTarget)
            case single :: Nil => toPhys[U](deps, planned0, single)
            case many =>
              val res = many.scanLeft((planned0, None: Option[PhysicalNode[U]])) { (hm, p) =>
                val (post, phys) = toPhys[U](deps, hm._1, p)
                (post, Some(phys))
              }
              (res.last._1, FanOut[U](res.collect { case (_, Some(phys)) => phys }))
          }

        def cast[A](out: (HMap[ProdCons, PhysicalNode], PhysicalNode[A])): (HMap[ProdCons, PhysicalNode], PhysicalNode[T]) = {
          out.asInstanceOf[(HMap[ProdCons, PhysicalNode], PhysicalNode[T])]
        }

        that match {
          case Source(source) =>
            def go[A](in: Source[A]) = {
              val (planned, targets) = maybeFanout[A]
              // We can only call this on sources if T == U, which we do
              val phys = SourceNode(in, targets)
              (planned + (that -> phys), phys)
            }
            cast(go(source))

          case FlatMappedProducer(_, fn) =>
            def go[A, B](fn: A => TraversableOnce[B]) = {
              val (planned, targets) = maybeFanout[B]
              val phys = FlatMap(fn, targets)
              (planned + (that -> phys), phys)
            }
            go(fn)

          case WrittenProducer(prod, queue) =>
            def go[T](in: Prod[T], sink: Sink[T]) = {
              val (planned, targets) = maybeFanout[T]
              val phys = Writer(sink, targets)
              (planned + (that -> phys), phys)
            }
            go(prod, queue)

          case LeftJoinedProducer(prod, service) =>
            def go[K, V, U](in: Prod[(K, U)], service: Service[K, V]) = {
              val (planned, targets) = maybeFanout[(K, (U, Option[V]))]
              val phys = Join(service, targets)
              (planned + (that -> phys), phys)
            }
            cast(go(prod, service))

          case Summer(producer, store, sg) => {
            def go[K, V](in: Prod[(K, V)], str: Store[K, V], semi: Semigroup[V]) = {
              val (planned, targets) = maybeFanout[(K, (Option[V], V))]
              val phys = Sum(str, semi, targets)
              (planned + (that -> phys), phys)
            }
            cast(go(producer, store, sg))
          }

          case other =>
            sys.error("%s encountered, which should have been optimized away".format(other))
        }
    }

  def plan[T](prod: TailProducer[ConcurrentMemory, T]): ConcurrentMemoryPlan = {
    /*
     * Register the counters
     */
    val registeredCounters: Seq[(Group, Name)] =
      JobCounters.getCountersForJob(jobID).getOrElse(Nil)

    if (!registeredCounters.isEmpty) {
      MemoryStatProvider.registerCounters(jobID, registeredCounters)
      SummingbirdRuntimeStats.addPlatformStatProvider(MemoryStatProvider)
    }
    /*
     * These rules should normalize the graph into a plannable state by the toPhys
     * recursive function (removing no-op nodes and converting optionMap and KeyFlatMap to just
     * flatMap)
     */
    val ourRule = OptionToFlatMap
      .orElse(KeyFlatMapToFlatMap)
      .orElse(FlatMapFusion)
      .orElse(RemoveNames)
      .orElse(RemoveIdentityKeyed)
      .orElse(ValueFlatMapToFlatMap)

    val deps = Dependants(optimize(prod, ourRule))
    val heads = deps.nodes.collect { case s @ Source(_) => s }
    heads.foldLeft((HMap.empty[ProdCons, PhysicalNode], NullPlan: ConcurrentMemoryPlan)) {
      case ((hm, plan), head) =>
        val (nextHm, plannedSource) = toPhys(deps, hm, head)
        // All sources should be planned to source nodes
        val sourceNode = plannedSource.asInstanceOf[SourceNode[_]]
        val nextPlan = Monoid.plus(plan, sourceNode)
        (nextHm, nextPlan)
    }._2
  }
}
