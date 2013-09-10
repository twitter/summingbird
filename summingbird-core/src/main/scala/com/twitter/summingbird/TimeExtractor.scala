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

package com.twitter.summingbird

/**
  * TimeExtractor is really just a function, but we've used a
  * specialized type for implicit resolution and serializability.
  */

object TimeExtractor {
  def apply[T](fn: T => Long): TimeExtractor[T] =
    new TimeExtractor[T] {
      override def apply(t: T) = fn(t)
    }
}

trait TimeExtractor[T] extends (T => Long) with java.io.Serializable
