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

import java.util.Date

/** strictly before the exclusiveBound, we use the before Batcher.
 * At the exclusiveBound, the batch increments abe switches to using
 * the after batcher. The BatchID WON'T be the same as what after
 * would produce, as we will subtract batchOf(exclusiveBound) from
 * the BatchID so that the BatchIDs are contiguous. The BatchID of
 * exclusiveBound is before.batchOf(exclusiveBound - 1ms) + 1
 */
class CombinedBatcher(before: Batcher,
  exclusiveBound: Date,
  after: Batcher) extends Batcher {

  val batchAtBound: BatchID = before.batchOf(new Date(exclusiveBound.getTime - 1L)) + 1L
  val afterBatchDelta: BatchID = after.batchOf(exclusiveBound)

  def batchOf(d: Date): BatchID =
    if(d.compareTo(exclusiveBound) >= 0) {
      (after.batchOf(d) - afterBatchDelta) + batchAtBound
    }
    else {
      before.batchOf(d)
    }

  def earliestTimeOf(b: BatchID): Date =
    if(b > batchAtBound) {
      after.earliestTimeOf((b - batchAtBound) + afterBatchDelta)
    }
    else if(b == batchAtBound) {
      new Date(exclusiveBound.getTime) // damn mutable dates
    }
    else
      before.earliestTimeOf(b)
}
