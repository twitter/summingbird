package com.twitter.summingbird.bijection

import com.twitter.algebird.Monoid
import com.twitter.bijection.Bijection

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object BijectionImplicits {
  implicit def enriched[T,U](bij: Bijection[T,U]) = new BijectionEnrichment(bij)
}

// TODO: Move to algebird.

class BijectionEnrichment[T,U](bij: Bijection[T,U]) extends java.io.Serializable {
  def lift(monoid: Monoid[T]) = new Monoid[U] {
    override lazy val zero = bij(monoid.zero)
    override def plus(x: U, y: U) = {
      bij(monoid.plus(bij.invert(x), bij.invert(y)))
    }
  }
}
