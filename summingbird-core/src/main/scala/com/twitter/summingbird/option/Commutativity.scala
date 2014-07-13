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

package com.twitter.summingbird.option

import java.io.Serializable

// TODO: this functionality should be in algebird:
// https://github.com/twitter/algebird/issues/128
sealed trait Commutativity extends Serializable
case object NonCommutative extends Commutativity
case object Commutative extends Commutativity

/**
 * A readable way to specify commutivity in a way
 * that works with the Class-based Options system.
 * We do this so we can use the classOf[MonoidIsCommutative]
 * as the key for the option.
 */
object MonoidIsCommutative {
  /**
   * Assume false unless the user says otherwise
   */
  val default = MonoidIsCommutative(NonCommutative)
  /**
   * True if the Monoid is commutative, false otherwise.
   */
  def apply(isCommutative: Boolean): MonoidIsCommutative =
    if (isCommutative) {
      MonoidIsCommutative(Commutative)
    } else {
      default
    }
}
case class MonoidIsCommutative(commutativity: Commutativity)
