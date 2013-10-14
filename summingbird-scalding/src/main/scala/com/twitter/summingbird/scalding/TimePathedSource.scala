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

import com.twitter.scalding.{TimePathedSource => sTPS, DateRange, AbsoluteDuration}

object TimePathedSource extends java.io.Serializable {
  // Assumes linear expanders
  private def unexpander(init: DateRange, expander: DateRange => DateRange): (DateRange) => Option[DateRange] = {
    val expanded = expander(init)
    val sdiff = expanded.start - init.start
    val ediff = expanded.end - init.end;
    { (dr: DateRange) =>
      val newStart = dr.start - sdiff
      val newEnd = dr.end - ediff
      if(newStart > newEnd) None else Some(DateRange(newStart, newEnd))
    }
  }

  @annotation.tailrec
  private def minifyRec(init: DateRange,
    expander: DateRange => DateRange,
    vertractor: DateRange => Option[DateRange]): Option[DateRange] = {
      val expanded = expander(init)
      val unex = unexpander(init, expander)
      vertractor(expanded) match {
        case None => None
        case Some(rt) if rt.contains(expanded) => Some(init) // we can satisfy init
        case Some(subset) =>
          unex(subset) match {
            case None => None
            case Some(newInit) if newInit.contains(init) =>
              sys.error("DateRange expansion ill-behaved: %s -> %s -> %s -> %s".format(init, expanded, subset, newInit))
            case Some(newInit) => minifyRec(newInit, expander, vertractor)
          }
      }
  }

  def minify(expander: DateRange => DateRange,
    vertractor: DateRange => Option[DateRange]): (DateRange => Option[DateRange]) =
      { (init: DateRange) => minifyRec(init, expander, vertractor) }
}
