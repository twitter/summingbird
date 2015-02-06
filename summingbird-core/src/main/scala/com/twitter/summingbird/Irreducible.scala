/*
 Copyright 2014 Twitter, Inc.

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

package com.twitter.summingbird

import com.twitter.algebird.{ Semigroup => AlgebirdSemigroup }

/**
 * An Irreducible is something that was passed from the user
 * which we cannot look inside of: a source, a function, store,
 * service, sink.
 */
sealed trait Irreducible[P <: Platform[P]]

object Irreducible {
  // Namespace these since they use common names:
  case class Source[P <: Platform[P]](source: P#Source[_]) extends Irreducible[P]
  case class Store[P <: Platform[P]](store: P#Store[_, _]) extends Irreducible[P]
  case class Sink[P <: Platform[P]](sink: P#Sink[_]) extends Irreducible[P]
  case class Service[P <: Platform[P]](service: P#Service[_, _]) extends Irreducible[P]
  case class Function[P <: Platform[P]](fn: Function1[_, _]) extends Irreducible[P]
  case class Semigroup[P <: Platform[P]](sg: AlgebirdSemigroup[_]) extends Irreducible[P]
}
