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

package com.twitter.summingbird.scalding.source

import com.twitter.scalding.{ TimePathedSource => STPS, _ }
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

// TODO move this to scalding:
// https://github.com/twitter/scalding/issues/529
object TimePathedSource extends java.io.Serializable {

  @transient private val logger = LoggerFactory.getLogger(TimePathedSource.getClass)

  // Assumes linear expanders
  private def unexpander(init: DateRange, expander: DateRange => DateRange): (DateRange) => Option[DateRange] = {
    val expanded = expander(init)
    val sdiff = expanded.start - init.start
    val ediff = expanded.end - init.end;
    { (dr: DateRange) =>
      val newStart = dr.start - sdiff
      val newEnd = dr.end - ediff
      if (newStart > newEnd) None else Some(DateRange(newStart, newEnd))
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

  def satisfiableHdfs(mode: Hdfs, desired: DateRange, fn: DateRange => STPS): Option[DateRange] = {
    val expander: (DateRange => DateRange) = fn.andThen(_.dateRange)

    val (tz, pattern) = {
      val test = fn(desired)
      (test.tz, test.pattern)
    }
    def toPath(date: RichDate): String =
      String.format(pattern, date.toCalendar(tz))

    val stepSize: Option[Duration] =
      List("%1$tH" -> Hours(1), "%1$td" -> Days(1)(tz),
        "%1$tm" -> Months(1)(tz), "%1$tY" -> Years(1)(tz))
        .find { unitDur: (String, Duration) => pattern.contains(unitDur._1) }
        .map(_._2)

    def allPaths(dateRange: DateRange): Iterable[(DateRange, String)] =
      stepSize.map {
        dateRange.each(_)
          .map { dr => (dr, toPath(dr.start)) }
      }
        .getOrElse(List((dateRange, pattern))) // This must not have any time after all

    def pathIsGood(p: String): Boolean = {
      val path = new Path(p)
      val valid = Option(path.getFileSystem(mode.conf).globStatus(path))
        .map(_.length > 0)
        .getOrElse(false)
      logger.debug("Tested input %s, Valid: %s. Conditions: Any files present, DateRange: %s".format(p, valid, desired))
      valid
    }

    val vertractor = { (dr: DateRange) =>
      allPaths(dr)
        .takeWhile { case (_, path) => pathIsGood(path) }
        .map(_._1)
        .reduceOption { (older, newer) => DateRange(older.start, newer.end) }
    }
    minify(expander, vertractor)(desired)
  }
}
