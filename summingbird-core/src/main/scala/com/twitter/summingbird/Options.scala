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

package com.twitter.summingbird

/**
  * intra-graph options.
  * Rather than use string keys, the .getClass of the option is used.
  * It is up to you to have classes that make sense and match what is consumed.
  */

object Options {
  def apply(opts: Map[Class[_], Any] = Map.empty): Options = new Options(opts)
}
class Options(val opts: Map[Class[_], Any]) {
  def set(opt: Any) = Options(opts + (opt.getClass -> opt))

  def get[T](klass: Class[T]): Option[T] =
    opts.get(klass).asInstanceOf[Option[T]]

  def getOrElse[T](klass: Class[T], default: T): T =
    opts.getOrElse(klass, default).asInstanceOf[T]

  private def klass[T: ClassManifest] = classManifest[T].erasure.asInstanceOf[Class[T]]

  def get[T: ClassManifest]: Option[T] = get(klass[T])
  def getOrElse[T: ClassManifest](default: T): T = getOrElse(klass[T], default)

  override def toString = "Options(%s)".format(opts.toString)
}
