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

package com.twitter.summingbird.online

import com.twitter.algebird.{ Semigroup, Tuple2Semigroup }
import com.twitter.storehaus.algebra.Mergeable
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.util.{ Future, Time }

// Cannot use a MergeableProxy here since we change the type.
class WrappedTSInMergeable[K, V](self: Mergeable[K, V]) extends Mergeable[K, (Timestamp, V)] {
  // Since we don't keep a timestamp in the store
  // this makes it clear that the 'right' or newest timestamp from the stream
  // will always be the timestamp outputted
  override val semigroup: Semigroup[(Timestamp, V)] =
    new Tuple2Semigroup()(Timestamp.rightSemigroup, self.semigroup)

  override def close(time: Time) = self.close(time)

  override def multiMerge[K1 <: K](kvs: Map[K1, (Timestamp, V)]): Map[K1, Future[Option[(Timestamp, V)]]] =
    self.multiMerge(kvs.mapValues(_._2)).map {
      case (k, futOpt) =>
        (k, futOpt.map { opt =>
          opt.map { v =>
            (kvs(k)._1, v)
          }
        })
    }
}
