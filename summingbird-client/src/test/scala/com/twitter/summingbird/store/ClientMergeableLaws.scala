package com.twitter.summingbird.store

import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.Store
import com.twitter.storehaus.algebra._
import com.twitter.summingbird.batch._
import com.twitter.util.{ Await, Future }

import com.twitter.algebird.Semigroup

import org.scalacheck._

object ClientMergeableLaws extends Properties("ClientMergeable") {

  implicit def batchArb: Arbitrary[BatchID] = Arbitrary(Gen.choose(0L, 100L).map(BatchID(_)))

  object Machine {
    def empty[K, V](implicit b: Batcher, sg: Semigroup[V]) =
      Machine(jstore[K, (BatchID, V)], jstore[(K, BatchID), V])
  }

  case class Machine[K, V](offline: Store[K, (BatchID, V)],
      online: Store[(K, BatchID), V])(implicit val batcher: Batcher, semi: Semigroup[V]) {
    val mergeable = ClientMergeable(offline, MergeableStore.fromStoreNoMulti(online), 10)
  }

  def jstore[K, V]: Store[K, V] =
    Store.fromJMap(new java.util.HashMap[K, Option[V]]())

  /**
   * This test prepares an initial offline store, and has a list of key, value1, value2 to merge.
   * Then we check that result of the first merge which should be the same of the result before.
   * Then after the second merge, the result should be the sum of the value1 and the initial.
   * lastly, the value should be the total sum.
   */
  property("get-merge works") = Prop.forAll { (init: Map[Int, Int], toMerge: List[(Int, (Int, Int))]) =>
    implicit val batcher = Batcher.ofHours(2)
    val machine = Machine.empty[Int, Int]
    init.foreach {
      case (k, v) =>
        machine.offline.put((k, Some((BatchID(0), v))))
    }
    toMerge.forall {
      case (k, (v1, v2)) =>
        val tup = for {
          prior <- machine.mergeable.readable.multiGetBatch(BatchID(1), Set(k))(k)
          mergePrior <- machine.mergeable.merge((k, BatchID(1)), v1)
          mergeAfter <- machine.mergeable.merge((k, BatchID(1)), v2)
          last <- machine.mergeable.readable.multiGetBatch(BatchID(1), Set(k))(k)
        } yield (prior, mergePrior, mergeAfter, last)

        Await.result(tup.map {
          case x @ (None, None, Some(o3), Some(last)) =>
            val r = o3 == v1 && (v1 + v2 == last)
            if (!r) println(x)
            r
          case x @ (Some(o1), Some(o2), Some(o3), Some(last)) =>
            val r = (o1 == o2) && (o2 + v1 == o3) && (o1 + v1 + v2 == last)
            if (!r) println(x)
            r
          case x =>
            println(x)
            false
        })
    }
  }

  property("sequential merges include prior") = Prop.forAll { (init: Map[Int, Int], toMerge: Map[Int, (Int, Int)]) =>
    implicit val batcher = Batcher.ofHours(2)
    val machine = Machine.empty[Int, Int]
    init.foreach {
      case (k, v) =>
        machine.offline.put((k, Some((BatchID(0), v))))
    }
    val keys: Map[(Int, BatchID), Int] = toMerge.flatMap {
      case (k, (v1, v2)) =>
        Map((k, BatchID(1)) -> v1, (k, BatchID(2)) -> v2)
    }

    Await.result(Future.collect(machine.mergeable.multiMerge(keys)
      .collect {
        case ((k, BatchID(2)), fopt) => fopt.map(_ == Some(toMerge(k)._1 + init.getOrElse(k, 0)))
      }
      .toSeq)
      .map(_.forall(identity)))
  }

  property("sequential merges include prior (single merge)") = Prop.forAll { (init: Map[Int, Int], toMerge: Map[Int, (Int, Int)]) =>
    implicit val batcher = Batcher.ofHours(2)
    val machine = Machine.empty[Int, Int]
    init.foreach {
      case (k, v) =>
        machine.offline.put((k, Some((BatchID(0), v))))
    }
    val keys: List[((Int, BatchID), Int)] = toMerge.toList.flatMap {
      case (k, (v1, v2)) =>
        Map((k, BatchID(1)) -> v1, (k, BatchID(2)) -> v2)
    }

    val merged = keys.map { kbv => (kbv._1, machine.mergeable.merge(kbv)) }

    Await.result(Future.collect(merged.collect {
      case ((k, BatchID(2)), fopt) => fopt.map(_ == Some(toMerge(k)._1 + init.getOrElse(k, 0)))
    })
      .map(_.forall(identity)))
  }

  property("simple check") = {
    implicit val b = Batcher.ofHours(23)
    val machine = Machine.empty[Int, Int]
    val merge = machine.mergeable
    // Initialize key 0 with some data:
    machine.offline.put((0, Some((BatchID(0), 1))))
    val prior1 = merge.merge(((0, BatchID(1)), 3))
    val first = Await.result(prior1) match {
      case Some(1) => true
      case x =>
        println("first:" + x)
        false
    }
    val prior2 = merge.merge(((0, BatchID(1)), 7))
    val second = Await.result(prior2) match {
      case Some(4) => true
      case x =>
        println("secord:" + x)
        false
    }
    val last = merge.readable.multiGetBatch(BatchID(1), Set(0)).apply(0)
    val third = Await.result(last) match {
      case Some(11) => true
      case x =>
        println("third:" + x)
        false
    }
    first && second && third
  }
}

