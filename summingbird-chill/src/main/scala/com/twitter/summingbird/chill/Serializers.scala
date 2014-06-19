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

package com.twitter.summingbird.chill
import com.twitter.chill._
import com.twitter.summingbird.batch.{ BatchID, Timestamp }

class BatchIDSerializer extends KSerializer[BatchID] {
  val optimizeForPositive = true
  def write(kser: Kryo, out: Output, batch: BatchID) {
    out.writeLong(batch.id, optimizeForPositive)
  }

  def read(kser: Kryo, in: Input, cls: Class[BatchID]): BatchID =
    BatchID(in.readLong(optimizeForPositive))
}

class TimestampSerializer extends KSerializer[Timestamp] {
  val optimizeForPositive = true
  def write(kser: Kryo, out: Output, ts: Timestamp) {
    out.writeLong(ts.milliSinceEpoch, optimizeForPositive)
  }

  def read(kser: Kryo, in: Input, cls: Class[Timestamp]): Timestamp =
    Timestamp(in.readLong(optimizeForPositive))
}
