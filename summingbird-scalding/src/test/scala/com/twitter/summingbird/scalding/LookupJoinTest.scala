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

import org.specs._
import java.lang.{Integer => JInt}
import com.twitter.scalding._

import com.twitter.algebird.{Monoid, Group}

object LookupJoinedTest {
  // Not defined if there is a collision in K and T, so make those unique:
  def genList(maxTime: Int, maxKey: Int, sz: Int): List[(Int, Int, Int)] = {
    val rng = new java.util.Random
    (0 until sz).view.map { _ =>
      (rng.nextInt(maxTime), rng.nextInt(maxKey), rng.nextInt)
    }
    .groupBy{case (t,k,v) => (t,k)}
    .mapValues(_.headOption.toList)
    .values
    .flatten
    .toList
  }
}

class LookupJoinerJob(args : Args) extends Job(args) {
  import TDsl._

  val in0 = TypedTsv[(Int,Int,Int)]("input0")
  val in1 = TypedTsv[(Int,Int,Int)]("input1")

  LookupJoin(TypedPipe.from(in0).map { case (t,k,v) => (t, (k, v)) },
    TypedPipe.from(in1).map { case (t,k,v) => (t, (k, v)) })
    .map { case (t, (k, (v, opt))) =>
      (t.toString, k.toString, v.toString, opt.toString)
    }
    .write(TypedTsv[(String,String,String,String)]("output"))
}

class LookupJoinedTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  import LookupJoinedTest.genList

  def lookupJoin[T:Ordering,K,V,W](in0: Iterable[(T,K,V)], in1: Iterable[(T,K,W)]) = {
    val serv = in1.groupBy(_._2)
    def lookup(t: T, k: K): Option[W] = {
      val ord = Ordering.by { tkw: (T, K, W) => tkw._1 }
      serv.get(k).flatMap { in1s =>
        in1s.filter { case (t1, _, _) => Ordering[T].lt(t1, t) }
          .reduceOption(ord.max(_, _))
          .map { _._3 }
      }
    }
    in0.map { case (t,k,v) => (t.toString, k.toString, v.toString, lookup(t, k).toString) }
  }
  "A LookupJoinerJob" should {
    //Set up the job:
    "correctly lookup" in {
      val MAX_KEY = 10
      val in0 = genList(Int.MaxValue, MAX_KEY, 10000)
      val in1 = genList(Int.MaxValue, MAX_KEY, 10000)
      JobTest(new LookupJoinerJob(_))
        .source(TypedTsv[(Int,Int,Int)]("input0"), in0)
        .source(TypedTsv[(Int,Int,Int)]("input1"), in1)
        .sink[(String, String, String, String)](
          TypedTsv[(String,String,String,String)]("output")) { outBuf =>
          outBuf.toSet must be_==(lookupJoin(in0, in1).toSet)
          in0.size must be_==(outBuf.size)
        }
        .run
        //.runHadoop
        .finish
    }
  }
}

class WindowLookupJoinerJob(args : Args) extends Job(args) {
  import TDsl._

  val in0 = TypedTsv[(Int,Int,Int)]("input0")
  val in1 = TypedTsv[(Int,Int,Int)]("input1")
  val window = args("window").toInt

  def gate(left: Int, right: Int) =
    (left.toLong - right.toLong) < window

  LookupJoin.withWindow(TypedPipe.from(in0).map { case (t,k,v) => (t, (k, v)) },
    TypedPipe.from(in1).map { case (t,k,v) => (t, (k, v)) })(gate _)
    .map { case (t, (k, (v, opt))) =>
      (t.toString, k.toString, v.toString, opt.toString)
    }
    .write(TypedTsv[(String,String,String,String)]("output"))
}

class WindowLookupJoinedTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  import LookupJoinedTest.genList
  def windowLookupJoin[K,V,W](in0: Iterable[(Int,K,V)], in1: Iterable[(Int,K,W)], win: Int) = {
    val serv = in1.groupBy(_._2)
    // super inefficient, but easy to verify:
    def lookup(t: Int, k: K): Option[W] = {
      val ord = Ordering.by { tkw: (Int, K, W) => tkw._1 }
      serv.get(k).flatMap { in1s =>
        in1s.filter { case (t1, _, _) =>
            (t1 < t) && ((t.toLong - t1.toLong) < win)
        }
        .reduceOption(ord.max(_, _))
        .map { _._3 }
      }
    }
    in0.map { case (t,k,v) => (t.toString, k.toString, v.toString, lookup(t, k).toString) }
  }
  "A WindowLookupJoinerJob" should {
    //Set up the job:
    "correctly lookup" in {
      val MAX_KEY = 10
      val MAX_TIME = 10000
      val in0 = genList(MAX_TIME, MAX_KEY, 10000)
      val in1 = genList(MAX_TIME, MAX_KEY, 10000)
      JobTest(new WindowLookupJoinerJob(_))
        .arg("window", "100")
        .source(TypedTsv[(Int,Int,Int)]("input0"), in0)
        .source(TypedTsv[(Int,Int,Int)]("input1"), in1)
        .sink[(String, String, String, String)](
          TypedTsv[(String,String,String,String)]("output")) { outBuf =>
          val results = outBuf.toList.sorted
          val correct = windowLookupJoin(in0, in1, 100).toList.sorted
          def some(it: List[(String,String,String,String)]) =
            it.filter(_._4.startsWith("Some"))

          def none(it: List[(String,String,String,String)]) =
            it.filter(_._4.startsWith("None"))

          some(results) must be_==(some(correct))
          none(results) must be_==(none(correct))
          in0.size must be_==(outBuf.size)
        }
        .run
        //.runHadoop
        .finish
    }
  }
}
