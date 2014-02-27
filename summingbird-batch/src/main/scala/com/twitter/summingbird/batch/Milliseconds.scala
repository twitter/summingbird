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

case class Milliseconds(toLong: Long) extends Ordered[Milliseconds] {
  def compare(that: Milliseconds) = toLong.compare(that.toLong)
  def prev = copy(toLong = toLong - 1)
  def next = copy(toLong = toLong + 1)
}

object Milliseconds {
  val Max = Milliseconds(Long.MaxValue)
  val Min = Milliseconds(Long.MinValue)
  implicit val orderingOnTimestamp: Ordering[Milliseconds] = Ordering.by(_.toLong)
}