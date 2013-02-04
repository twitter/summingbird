/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.batch

/**
* Batcher with a single, infinitely long batch. This makes sense for
* online-only jobs with a single store that will never need to merge
* at the batch level.
*
* We need to pass a value for a lower bound of any Time expected to
* be seen. The time doesn't matter, we just need some instance.
*
* @author Oscar Boykin
* @author Sam Ritchie
*/

object UnitBatcher {
  def apply[Time: Ordering](currentTime: Time): UnitBatcher[Time] = new UnitBatcher(currentTime)
}

class UnitBatcher[Time: Ordering](override val currentTime: Time) extends Batcher[Time] {
  def parseTime(s: String) = currentTime
  def earliestTimeOf(batch: BatchID) = currentTime
  def batchOf(t: Time) = BatchID(0)
  lazy val timeComparator = implicitly[Ordering[Time]]
}
