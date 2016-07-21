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

package com.twitter.summingbird.batch

import com.twitter.algebird.ExclusiveUpper

/**
 * strictly before the beforeBound, we use the before Batcher.
 * At the beforeBound, the batch increments abe switches to using
 * the after batcher. The BatchID WON'T be the same as what after
 * would produce, as we will subtract batchOf(beforeBound) from
 * the BatchID so that the BatchIDs are contiguous. The BatchID of
 * beforeBound is before.batchOf(beforeBound - 1ms) + 1
 */
class CombinedBatcher(before: Batcher,
    beforeBound: ExclusiveUpper[Timestamp],
    after: Batcher) extends Batcher {
  import OrderedFromOrderingExt._
  val batchAtBound: BatchID = before.batchOf(beforeBound.upper.prev) + 1L
  val afterBatchDelta: BatchID = after.batchOf(beforeBound.upper)

  def batchOf(d: Timestamp): BatchID =
    if (!beforeBound.contains(d)) {
      (after.batchOf(d) - afterBatchDelta.id) + batchAtBound.id
    } else {
      before.batchOf(d)
    }

  def earliestTimeOf(b: BatchID): Timestamp =
    if (b > batchAtBound) {
      after.earliestTimeOf((b - batchAtBound.id) + afterBatchDelta.id)
    } else if (b == batchAtBound) {
      beforeBound.upper
    } else
      before.earliestTimeOf(b)
}
