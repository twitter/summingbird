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

package com.twitter.summingbird.builder

import com.twitter.summingbird._
import org.specs2.mutable._

case class One()
case class Two()

class SourceBuilderTest extends Specification {
  "SourceBuilder.adjust should properly update a map" in {
    val empty = Map[String, Options]()

    empty.get("a") must be_==(None)
    empty.get("b") must be_==(None)

    val withA = SourceBuilder.adjust(empty, "a")(_.set(One()))

    withA.get("a").flatMap(_.get[One]).exists(Set(One())) must beTrue
    withA.get("b") must be_==(None)

    val withB = SourceBuilder.adjust(withA, "b")(_.set(Two()))

    withB.get("a").flatMap(_.get[One]).exists(Set(One())) must beTrue
    withB.get("b").flatMap(_.get[Two]).exists(Set(Two())) must beTrue
  }
}
