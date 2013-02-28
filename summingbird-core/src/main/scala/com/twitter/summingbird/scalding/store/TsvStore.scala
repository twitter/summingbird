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

package com.twitter.summingbird.scalding.store

import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.algebird.Monoid
import com.twitter.scalding.{Dsl, Mode, TypedPipe, TDsl, Tsv}
import cascading.flow.FlowDef

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

object TsvStore {
  def apply[Key, Value](path: String) = new TsvStore(path)
}

class TsvStore[Key, Value](path: String) extends BatchStore[Key, Value] {
  import Dsl._
  import TDsl._

  // TODO write metadata about the BatchID in alongside the path.
  override def readLatest(env: ScaldingEnv)
  (implicit fd: FlowDef, mode: Mode): (BatchID, TypedPipe[(Key,Value)]) =
    (BatchID(0), Tsv(path).read.toTypedPipe[(Key,Value)]((0,1)))

  override def write(env : ScaldingEnv, p : TypedPipe[(Key,Value)])
    (implicit fd : FlowDef, mode : Mode) {
    import Dsl._
    import TDsl._
    p.write(('key, 'value), Tsv(path))
  }

  override def commit(batchid : BatchID, env : ScaldingEnv) { }
}
