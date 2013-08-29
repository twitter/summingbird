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
 * We put typedefs and vals here to make working with the (deprecated) Builder
 * API nicer.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */
package object builder {
  val MonoidIsCommutative = com.twitter.summingbird.option.MonoidIsCommutative
  type MonoidIsCommutative = com.twitter.summingbird.option.MonoidIsCommutative

  /** Scalding options here */
  val FlatMapShards = com.twitter.summingbird.scalding.option.FlatMapShards
  type FlatMapShards = com.twitter.summingbird.scalding.option.FlatMapShards
  val Reducers = com.twitter.summingbird.scalding.option.Reducers
  type Reducers = com.twitter.summingbird.scalding.option.Reducers
}
