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
  * Base trait for summingbird compilers.
  */
trait Platform[P <: Platform[P]] {
  // The type of the inputs for this platform
  type Source[+t]
  type Store[-k, v]
  type Sink[-t]
  type Service[-k, +v]
  type Plan[_]

  def plan[T](completed: Producer[P, T]): Plan[T]
}
