

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

import com.twitter.algebird.monad._
import com.twitter.summingbird.batch._

import com.twitter.scalding.{ Source => ScaldingSource, Test => TestMode, _ }
import com.twitter.summingbird.{Producer, TimeExtractor }
import scala.collection.mutable.Buffer
import cascading.tuple.Tuple
import cascading.flow.FlowDef

object TestSource {
    def apply[T](iter: Iterable[T])
      (implicit mf: Manifest[T], te: TimeExtractor[T], tc: TupleConverter[T], tset: TupleSetter[T]):
        (Map[ScaldingSource, Buffer[Tuple]], Producer[Scalding, T]) = {
      val src = IterableSource(iter)
      val prod = Scalding.sourceFromMappable { _ => src }
      (Map(src -> iter.map { tset(_) }.toBuffer), prod)
  }
}
