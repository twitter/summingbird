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

import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.scalding.{Mode, TypedPipe, Grouped}
import cascading.flow.FlowDef

// TODO this functionality should be in algebird
sealed trait Commutativity extends java.io.Serializable
object NonCommutative extends Commutativity
object Commutative extends Commutativity

trait ScaldingStore[K, V] {
  implicit def ordering: Ordering[K]

  def writeDeltas(batchID: BatchID, delta: KeyValuePipe[K, V])(implicit flowdef: FlowDef, mode: Mode): Unit

  /** Optionally write out the stream of output values (for use as a service)
   */
  def writeStream(batchID: BatchID, scanned: KeyValuePipe[K, V])(implicit flowdef: FlowDef, mode: Mode): Unit
  /** Write the stream which only has the last value for each key
   */
  def writeLast(batchID: BatchID, scanned: KeyValuePipe[K, V])(implicit flowdef: FlowDef, mode: Mode): Unit

  /** Read the latest value for the keys
   * Each key appears at most once
   * May include keys from previous batches if those keys have not been updated
   */
  def readLatestBefore(batchID: BatchID)(implicit flowdef: FlowDef, mode: Mode): KeyValuePipe[K, V]

  /**
    * Accepts deltas along with their timestamps, returns triples of
    * (time, K, V(aggregated up to the time)).
    *
    * Same return as lookup on a ScaldingService.
    */
  def merge(batchID: BatchID,
    delta: KeyValuePipe[K, V],
    commutativity: Commutativity,
    reducers: Int = -1)(implicit flowdef: FlowDef, mode: Mode, sg: Semigroup[V]): KeyValuePipe[K, V] = {

    writeDeltas(batchID, delta)

    //Convert to a Grouped by "swapping" Time and K
    def toGrouped(items: KeyValuePipe[K, V]): Grouped[K, (Long, V)] =
      items.groupBy { case (_, (k, _)) => k }
        .mapValues { case (t, (_, v)) => (t, v) }
        .withReducers(reducers)
    //Unswap the Time and K
    def toKVPipe(tp: TypedPipe[(K, (Long, V))]): KeyValuePipe[K, V] =
      tp.map { case (k, (t, v)) => (t, (k, v)) }

    val grouped: Grouped[K, (Long, V)] = toGrouped(readLatestBefore(batchID) ++ delta)

    val sorted = grouped.sortBy { _._1 } // sort by time
    val maybeSorted = commutativity match {
      case Commutative => grouped // order does not matter
      case NonCommutative => sorted
    }

    val redFn: (((Long, V), (Long, V)) => (Long, V)) = { (left, right) =>
      val (tl, vl) = left
      val (tr, vr) = right
      (tl max tr, Semigroup.plus(vl, vr))
    }
    // could be empty, in which case scalding will do nothing here
    writeLast(batchID, toKVPipe(maybeSorted.reduce(redFn)))

    // Make the incremental stream
    val stream = toKVPipe(sorted.scanLeft(None: Option[(Long, V)]) { (old, item) =>
        old match {
          case None => Some(item)
          case Some(prev) => Some(redFn(prev, item))
        }
      }
      .mapValueStream { _.flatten /* unbox the option */ }
      .toTypedPipe
    )
    // could be empty, in which case scalding will do nothing here
    writeStream(batchID, stream)
    stream
  }
}

