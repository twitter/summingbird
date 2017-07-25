package com.twitter.summingbird

import org.scalacheck.{ Arbitrary, Gen }

/**
 * [[org.scalacheck.GenArities]] and [[org.scalacheck.ArbitraryArities]] classes from scalacheck
 * contains to many lambdas in the same file which leads to
 * bug with functions serialization in Scala 2.12: https://issues.scala-lang.org/browse/SI-10232
 *
 * As a workaround this class contains overriden implicits for such cases.
 * Should be imported if you import [[Arbitrary]] class.
 *
 * ScalaCheck issue is tracked at https://github.com/rickynils/scalacheck/issues/342.
 */
object ArbitraryWorkaround {
  implicit val f1: Arbitrary[Int => Int] = Arbitrary(Gen.const(x => x * 2))
  implicit val f2: Arbitrary[Int => List[(Int, Int)]] = Arbitrary(Gen.const(x => List((x, x * 3))))
  implicit val f3: Arbitrary[Int => Option[Int]] = Arbitrary(Gen.const(x => {
    if (x % 2 == 0) None else Some(x * 4)
  }))
  implicit val f4: Arbitrary[Int => List[Int]] = Arbitrary(Gen.const(x => List(x * 5)))
  implicit val f5: Arbitrary[((Int, (Int, Option[Int]))) => List[(Int, Int)]] = Arbitrary(Gen.const {
    case (x, (y, optZ)) => List((x, y), (x, optZ.getOrElse(42)))
  })
  implicit val f6: Arbitrary[((Int, Int)) => List[(Int, Int)]] = Arbitrary(Gen.const(x => List(x, x)))
}
