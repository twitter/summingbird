/*
Copyright 2014 Twitter, Inc.

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

trait PrunedSpace[-T] extends java.io.Serializable {
  // expire (REMOVE) before writing, T is often (K, V) pair
  def prune(item: T, writeTime: Timestamp): Boolean
}

object PrunedSpace extends java.io.Serializable {
  val neverPruned: PrunedSpace[Any] =
    new PrunedSpace[Any] { def prune(item: Any, writeTime: Timestamp) = false }

  def apply[T](pruneFn: (T, Timestamp) => Boolean) = new PrunedSpace[T] {
    def prune(item: T, writeTime: Timestamp) = pruneFn(item, writeTime)
  }
}