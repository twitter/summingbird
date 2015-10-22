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

import org.scalatest.WordSpec

class BatcherSpec extends WordSpec {
  val hourlyBatcher = Batcher.ofHours(1)
  import OrderedFromOrderingExt._
  def assertRelation(other: Batcher, m: Map[Long, Iterable[Long]]) =
    m.foreach {
      case (input, expected) =>
        assert(other.enclosedBy(BatchID(input), hourlyBatcher).toList ==
          expected.map(BatchID(_)).toList)
    }

  "DurationBatcher should properly enclose a smaller, offset batcher" in {
    assertRelation(Batcher.ofMinutes(22), Map(
      0L -> List(0L), // 0 -> 22 minutes
      1L -> List(0L), // 23 -> 44 minutes
      2L -> List(0L, 1L), // 45 -> 1 hr, 6 minutes
      3L -> List(1L), // 1 hr, 7 minutes -> 1 hr, 28 minutes
      4L -> List(1L), // 1 hr, 29 minutes -> 1 hr, 50 minutes
      5L -> List(1L, 2L) // 1 hr, 51 minutes -> 2 hr, 12 minutes
    ))
  }

  "DurationBatcher when called on current batch should be within the last few seconds" in {
    val batcher = Batcher.ofMinutes(10)
    val longBatch: BatchID = batcher.currentBatch
    val curBatch: BatchID = batcher.batchOf(Timestamp.now)
    // For a 10min window, longBatch <= curBatch <= longBatch + 1
    assert((longBatch.compare(curBatch - 1) >= 0) == true)
    assert((longBatch.compare(curBatch) <= 0) == true)
  }

  "DurationBatcher should always require n batches to fit into a batcher of n hours" in {
    (10 to 100).foreach { n =>
      assert(Batcher.ofHours(n).enclosedBy(BatchID(100), hourlyBatcher).size == n)
    }
  }
}
