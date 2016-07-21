package com.twitter.summingbird.planner

import org.scalacheck._
import org.scalacheck.Prop.forAll

object ComposedFunctionsTest extends Properties("ComposedFunctions") {

  property("KeyFlatMapFunction maps only keys") =
    forAll { (f: Int => Iterable[Int], i: Int, s: String) =>
      val fn = KeyFlatMapFunction[Int, Int, String](f)
      fn((i, s)).toList == f(i).map((_, s)).toList
    }
  property("ValueFlatMapFunction maps only values") =
    forAll { (f: Int => Iterable[Int], i: Int, s: String) =>
      val fn = ValueFlatMapFunction[String, Int, Int](f)
      fn((s, i)).toList == f(i).map((s, _)).toList
    }
}

