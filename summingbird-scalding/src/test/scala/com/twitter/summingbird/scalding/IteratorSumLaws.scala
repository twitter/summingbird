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

package com.twitter.summingbird.scalding

import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._
import scala.math._

object IteratorSumLaws extends Properties("IteratorSumLaws") {
  import IteratorSums._

  property("groupedSum never increases size") = forAll { (in: List[(Int, Long)]) =>
    groupedSum(in.iterator).size <= in.size
  }
  property("groupedSum never empty if input is non-empty") = forAll { (in: List[(Int, Long)]) =>
    val res = groupedSum(in.iterator).toList
    (in.isEmpty && res.isEmpty) || (res.size > 0)
  }
  property("groupedSum works like groupBy + sum on sorted values") = forAll { (in: List[(Int, Long)]) =>
    val sorted = in.sorted
    groupedSum(sorted.iterator).toMap == in.groupBy { _._1 }.mapValues { kvs => kvs.map(_._2).sum }
  }
  property("partials passes through keys-values") = forAll { (in: List[(Int, Long)]) =>
    partials(in.iterator).map { case (k, (o, v)) => (k, v) }.toList == in
  }
  property("partials gives partial sums") = forAll { (in: List[(Int, Long)]) =>
    val s = partials(in.iterator).toList
    in.isEmpty || ( s.last._2._1.getOrElse(0L) == (in.dropRight(1).map(_._2).sum) )
  }
}
