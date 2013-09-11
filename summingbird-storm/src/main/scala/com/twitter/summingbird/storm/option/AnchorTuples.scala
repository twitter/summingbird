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

package com.twitter.summingbird.storm.option

import java.io.Serializable

/**
 *
 * If true, the topology will anchor tuples in all flatMap bolts and
 * ack in the final sink bolt.
 *
 * @author Sam Ritchie
 */

case class AnchorTuples(anchor: Boolean) extends Serializable

object AnchorTuples {
  val default = AnchorTuples(false)
}
