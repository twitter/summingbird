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

import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.scalding.{Mode, TypedPipe, AbsoluteDuration}
import com.twitter.algebird.monad.Reader
import cascading.flow.FlowDef

/**
 * This is a service that has a finite memory. There is a materialized
 * stream on disk, but the service only serves the data if:
 *  0 < t(incoming key) - t(service key) < t(window)
 * To use this you need to implement:
 * windowSize
 * readStream
 * batcher
 * ordering
 * reducers
 */
trait BatchedWindowService[K, V] extends BatchedService[K, V] {
  /**
   * A request must come in LESS than this window since the last
   * key written to the service
   */
  def windowSize: Time

  /**
   * The batched window never reads an aggregated last. Instead we just output
   * an empty pipe that is outside the window.
   */
  def readLast(exclusiveUB: BatchID, mode: Mode):  Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    val earliestInput = batcher.earliestTimeOf(exclusiveUB).milliSinceEpoch
    val earliestNeededKey = earliestInput - windowSize
    // We may need values from this batch:
    val earliestNeededBatch = batcher.batchOf(Timestamp(earliestNeededKey))
    // But all of these values are definitly too old:
    val firstZeroBatch = earliestNeededBatch.prev
    Right((firstZeroBatch, Scalding.emptyFlowProducer))
  }

  /** This executes the join algortihm on the streams.
   * You are guaranteed that all the service data needed
   * to do the join is present
   */
  override def lookup[W](incoming: TypedPipe[(Time, (K, W))],
    servStream: TypedPipe[(Time, (K, Option[V]))]): TypedPipe[(Time, (K, (W, Option[V])))] = {

    def flatOpt[T](o: Option[Option[T]]): Option[T] = o.flatMap(identity)

    implicit val ord = ordering
    val win = windowSize // call this once so scala makes a smarter closure
    LookupJoin.withWindow(incoming, servStream, reducers) { (l: Time, r: Time) => (l-r) < win }
      .map { case (t, (k, (w, optoptv))) => (t, (k, (w, flatOpt(optoptv)))) }
  }

}

/** More familiar interface to scalding users that creates
 * the Reader from two other methods
 */
trait SimpleWindowedService[K, V] extends BatchedWindowService[K, V] {
  def streamIsAvailable(b: BatchID, m: Mode): Boolean
  def read(b: BatchID)(implicit f: FlowDef, m: Mode): TypedPipe[(Time, (K, Option[V]))]

  final def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, Option[V])]] = {
    if(!streamIsAvailable(batchID, mode)) {
      None
    }
    else Some(Reader({ implicit fdm: (FlowDef, Mode) => read(batchID) }))
  }
}
