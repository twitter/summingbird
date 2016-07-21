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

object OrderedFromOrderingExt {
  // Use an implicit class to give the ordering operations on T
  // This lets us use an Ordering similar to the convenince of an Ordered
  implicit class HelperClass[T](val ts: T) extends AnyVal {
    def >(other: T)(implicit ord: Ordering[T]) = ord.gt(ts, other)
    def <(other: T)(implicit ord: Ordering[T]) = ord.lt(ts, other)
    def <=(other: T)(implicit ord: Ordering[T]) = ord.lteq(ts, other)
    def >=(other: T)(implicit ord: Ordering[T]) = ord.gteq(ts, other)
    def compare(other: T)(implicit ord: Ordering[T]) = ord.compare(ts, other)
  }
}
