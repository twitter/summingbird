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

/**
 * Batcher implementation based on a fixed-width batch of
 * milliseconds.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

class MillisecondBatcher(val durationMillis: Long) extends AbstractBatcher {
  require(durationMillis > 0, "a batch must have a non-zero size")
  def batchOf(t: Timestamp) = {
    val timeInMillis = t.milliSinceEpoch
    val batch = BatchID(timeInMillis / durationMillis)

    // Because long division rounds to zero instead of rounding down
    // toward negative infinity, a negative timeInMillis will
    // produce the BatchID AFTER the proper batch. To correct for
    // this, subtract a batch.
    if (timeInMillis < 0L) batch.prev else batch
  }

  def earliestTimeOf(batch: BatchID) = {
    val id = batch.id
    // Correct for the rounding-to-zero issue described above.
    if (id >= 0L)
      Timestamp(id * durationMillis)
    else
      Timestamp(id * durationMillis + 1L)
  }
}
