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

import java.util.Comparator
import com.twitter.bijection.Bijection

/**
 * For the purposes of batching, each Event object has exactly one
 * Time. The Batcher[Time] uses this time to assign each Event to a
 * specific BatchID. Batcher[Time] also has a Comparator[Time] so that
 * Time is ordered. Lastly, a Batcher[Time] can return the minimum
 * value of Time for each BatchID.  The Time type in practice might be
 * a java Date, a Long representing millis since the epoch, or an Int
 * representing seconds since the epoch, for instance.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object Batcher {
  import Bijection.asMethod

  /**
   * Returns a Bijection between Batcher[T] and Batcher[U].
   */
  implicit def bijection[T, U](implicit bij: Bijection[T, U]): Bijection[Batcher[T], Batcher[U]] =
    new BatcherBijection[T, U]

  /**
   * Converts a Batcher[T] into a Batcher[U] via an implicit bijection.
   */
  implicit def viaBijection[T, U](batcher: Batcher[T])(implicit bij: Bijection[T, U]): Batcher[U] =
    batcher.as[Batcher[U]]
}

trait Batcher[Time] extends java.io.Serializable {
  //  parseTime is used in Offline mode when the arguments to the job
  //  are strings which must be parsed. We need to have a way to convert
  //  from String to the Time type.
  def parseTime(s: String): Time
  def earliestTimeOf(batch: BatchID): Time
  def currentTime: Time
  def batchOf(t: Time): BatchID
  def timeComparator: Comparator[Time]

  def currentBatch = batchOf(currentTime)
}

/**
 * Abstract class to extend for easier java interop.
 */
abstract class AbstractBatcher[Time] extends Batcher[Time]
