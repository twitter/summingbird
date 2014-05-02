package com.twitter.summingbird.spark

import com.twitter.algebird.{Interval, Monoid}
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.option.{NonCommutative, Commutative, Commutativity}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

// TODO: add the logic for seeing what timespans each source / store / service can cover

// A source promises to never return values outside of timeSpan
trait SparkSource[T] {
  def rdd(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(Timestamp, T)]
}

trait SparkSink[T] {
  def write(sc: SparkContext, rdd: RDD[(Timestamp, T)], timeSpan: Interval[Timestamp]): Unit
}

trait SparkStore[K, V] {
  def merge(sc: SparkContext,
            timeSpan: Interval[Timestamp],
            deltas: RDD[(Timestamp, (K, V))],
            commutativity: Commutativity,
            monoid: Monoid[V])
            (implicit tag: ClassTag[K]): RDD[(Timestamp, (K, (Option[V], V)))]

  def write(sc: SparkContext, updatedSnapshot: RDD[(Timestamp, (K, (Option[V], V)))]): Unit
}

trait SimpleSparkStore[K, V] extends SparkStore[K, V] {

  // contract is:
  // no duplicate keys
  // provides a view of the world as of exactly the last instant of timeSpan
  // TODO: replace with batched logic (combine snapshots with deltas etc)
  def snapshot(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(K, V)]

  override def merge(sc: SparkContext,
                     timeSpan: Interval[Timestamp],
                     deltas: RDD[(Timestamp, (K, V))],
                     commutativity: Commutativity,
                     monoid: Monoid[V])
                     (implicit tag: ClassTag[K]): RDD[(Timestamp, (K, (Option[V], V)))] = {

    val snapshotRdd = snapshot(sc, timeSpan)

    val summedDeltas: RDD[(K, (Timestamp, V))] = commutativity match {
      case Commutative => {
        val keyedDeltas = deltas.map {
          case (ts, (k, v)) => (k, (ts, v))
        }
        keyedDeltas.reduceByKey {
          (acc, n) =>
            (acc, n) match {
              case ((highestTs, sumSoFar), (ts, v)) => (Ordering[Timestamp].max(highestTs, ts), monoid.plus(sumSoFar, v))
            }
        }
      }

      case NonCommutative => {
        val keyedDeltas = deltas.map {
          case (ts, (k, v)) => (k, (ts, v))
        }
        // TODO: how to get a sorted group w/o brining the entire group into memory?
        val grouped = keyedDeltas.groupByKey()
        grouped.map {
          case (k, vals) => {
            val sortedVals = vals.sortBy {
              case (ts, v) => ts
            }
            val maxTs = sortedVals.last._1
            val projectedSortedVals = sortedVals.iterator.map {
              case (ts, v) => v
            }
            // projectedSortedVals should never be empty so .get is safe
            (k, (maxTs, monoid.sumOption(projectedSortedVals).get))
          }
        }
      }
    }

    summedDeltas
      .leftOuterJoin(snapshotRdd)
      .map { case (k, ((ts, delta), storedValue)) =>
        val sum = storedValue.fold(delta)(sv => monoid.plus(sv, delta))
        (ts, (k, (storedValue, sum)))
      }
  }
}

trait SparkService[K, LV] {
  def lookup[V](sc: SparkContext,
                timeSpan: Interval[Timestamp],
                rdd: RDD[(Timestamp, (K, V))]): RDD[(Timestamp, (K, (V, Option[LV])))]
}