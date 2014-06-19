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

package com.twitter.summingbird.viz

import java.io.Writer
import com.twitter.summingbird.{ Platform, Producer, Dependants, NamedProducer, IdentityKeyedProducer }
import com.twitter.summingbird.planner._

object VizGraph {
  def apply[P <: Platform[P]](dag: Dag[P], writer: Writer): Unit = writer.write(apply(dag))
  def apply[P <: Platform[P]](dag: Dag[P]): String = DagViz(dag).toString
  def apply[P <: Platform[P]](tail: Producer[P, _], writer: Writer): Unit = writer.write(VizGraph(tail))
  def apply[P <: Platform[P]](tail: Producer[P, _]): String = ProducerViz(tail).toString
}
