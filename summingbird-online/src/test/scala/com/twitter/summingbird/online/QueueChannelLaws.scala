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

package com.twitter.summingbird.online

import org.scalacheck._
import Gen._
import Arbitrary._
import org.scalacheck.Prop._

import com.twitter.util.{Return, Throw, Future}

object QueueChannelLaws extends Properties("Queue and Channel") {

  property("Putting into a BoundedQueue gets size right") = forAll { (items: List[String]) =>
    val q = new BoundedQueue[String](10)
    q.putAll(items)
    q.size == items.size
  }
  property("not spill if capacity is enough") = forAll { (items: List[Int]) =>
    val q = new BoundedQueue[Int](items.size)
    q.putAll(items)
    q.spill.size == 0
  }
  property("Work with indepent additions") = forAll { (items: List[Int]) =>
    val q = new BoundedQueue[Int](items.size)
    items.map(q.put(_)) == (1 to items.size).toList
  }
  property("spill all with zero capacity") = forAll { (items: List[Int]) =>
    val q = new BoundedQueue[Int](0)
    q.putAll(items)
    q.spill == items
  }
  property("FutureChannel works with finished futures") = forAll { (items: List[Int]) =>
    val q = FutureChannel.linkedBlocking[Int,Int]
    items.foreach { i => q.put(i, Future(i*i)) }
    q.foldLeft((0, true)) { case ((cnt, good), (i, ti)) =>
      ti match {
        case Return(ii) => (cnt + 1, good)
        case Throw(e) => (cnt + 1, false)
      }
    } == (items.size, true)
  }
  property("FutureChannel.linkedNonBlocking works with finished futures") = forAll { (items: List[Int]) =>
    val q = FutureChannel.linkedNonBlocking[Int,Int]
    items.foreach { i => q.put(i, Future(i*i)) }
    q.foldLeft((0, true)) { case ((cnt, good), (i, ti)) =>
      ti match {
        case Return(ii) => (cnt + 1, good)
        case Throw(e) => (cnt + 1, false)
      }
    } == (items.size, true)
  }
  property("FutureChannel foreach works") = forAll { (items: List[Int]) =>
    // Make sure we can fit everything
    val q = FutureChannel.arrayBlocking[Int,Int](items.size + 1)
    items.foreach { q.call(_) { i => Future(i*i) } }
    var works = true
    q.foreach { case (i, Return(ii)) =>
      works = works && (ii == i*i)
    }
    works
  }
}
