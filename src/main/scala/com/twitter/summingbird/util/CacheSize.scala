package com.twitter.summingbird.util

import scala.util.Random

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * Accepts a lower bound and a percentage of fuzz. The internal
 * "size" function returns a random integer between the lower bound
 * and the fuzz percentage above that bound.
 *
 *
 */

case class CacheSize(lowerBound: Int, fuzz: Double = 0.2) extends java.io.Serializable {
  def size: Option[Int] =
    Some(lowerBound)
      .filter { _ > 0 }
      .map { s => s + Random.nextInt((fuzz * s).toInt)  }
}
