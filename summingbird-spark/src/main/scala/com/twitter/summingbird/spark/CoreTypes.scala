package com.twitter.summingbird.spark

import com.twitter.algebird.{Interval, Monoid}
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.option.{NonCommutative, Commutative, Commutativity}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.reflect.ClassTag

// TODO: add the logic for seeing what timespans each source / store / service can cover

// A source promises to never return values outside of timeSpan
trait SparkSource[T] extends Serializable {
  def rdd(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(Timestamp, T)]
}

trait SparkSink[T] extends Serializable {
  def write(sc: SparkContext, rdd: RDD[(Timestamp, T)], timeSpan: Interval[Timestamp]): Unit
}

case class MergeResult[K, V](
  sumBeforeMerge: RDD[(Timestamp, (K, (Option[V], V)))],
  writeClosure: () => Unit
)

trait SparkStore[K, V] extends Serializable {

  def merge(sc: SparkContext,
            timeSpan: Interval[Timestamp],
            deltas: RDD[(Timestamp, (K, V))],
            commutativity: Commutativity,
            monoid: Monoid[V]): MergeResult[K, V]
}

abstract class SimpleSparkStore[K: ClassTag, V: ClassTag] extends SparkStore[K, V] {

  // contract is:
  // no duplicate keys
  // provides a view of the world as of exactly the last instant of timeSpan
  // TODO: replace with batched logic (combine snapshots with deltas etc)
  def snapshot(sc: SparkContext, timeSpan: Interval[Timestamp]): RDD[(K, V)]

  def write(sc: SparkContext, updatedSnapshot: RDD[(K, V)]): Unit

  override def merge(sc: SparkContext,
                     timeSpan: Interval[Timestamp],
                     deltas: RDD[(Timestamp, (K, V))],
                     commutativity: Commutativity,
                     monoid: Monoid[V]): MergeResult[K, V] = {

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
        // TODO: how to get a sorted group w/o bringing the entire group into memory?
        //       *does* this even bring it into memory?
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

    val grouped = summedDeltas.cogroup(snapshotRdd)

    val summed = grouped
      .map { case (k, (deltaSeq, storedValueSeq)) =>
        val delta = deltaSeq.headOption
        val storedValue = storedValueSeq.headOption
        val sum = delta.map { d => storedValue.fold(d)(sv => (d._1, monoid.plus(sv, d._2))) }
        (k, storedValue, sum)
      }

    val sumBeforeMerge = summed
      .flatMap { case (k, storedValueOpt, sumOpt) =>
        sumOpt.map { case (ts, sum) => (ts, (k, (storedValueOpt, sum))) }
    }

    val updatedSnapshot = summed.map { case (k, storedValue, sum) =>
      val v = sum.map(_._2).getOrElse { storedValue.getOrElse(sys.error("this should never happen")) }
      (k, v)
    }

    val writeClosure = () => {
      write(sc, updatedSnapshot)
    }

    MergeResult(sumBeforeMerge, writeClosure)
  }
}

trait SparkService[K, LV] extends Serializable {
  def lookup[V](sc: SparkContext,
                timeSpan: Interval[Timestamp],
                rdd: RDD[(Timestamp, (K, V))]): RDD[(Timestamp, (K, (V, Option[LV])))]
}