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

package com.twitter.summingbird.scalding.service

import com.twitter.summingbird.Timestamp
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.scalding._
import com.twitter.scalding.{Mode, TypedPipe, AbsoluteDuration}
import com.twitter.algebird.monad.Reader
import cascading.flow.FlowDef

/** More familiar interface to scalding users that creates
 * the Reader from two other methods
 */
trait SimpleWindowedService[K, V] extends BatchedWindowService[K, V] {
  def streamIsAvailable(b: BatchID, m: Mode): Boolean
  def read(b: BatchID)(implicit f: FlowDef, m: Mode): TypedPipe[(Timestamp, (K, Option[V]))]

  final def readStream(batchID: BatchID, mode: Mode): Option[FlowToPipe[(K, Option[V])]] = {
    if(!streamIsAvailable(batchID, mode)) {
      None
    }
    else Some(Reader({ implicit fdm: (FlowDef, Mode) => read(batchID) }))
  }
}