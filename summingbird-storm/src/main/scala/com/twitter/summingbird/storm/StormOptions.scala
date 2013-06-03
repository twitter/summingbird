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

package com.twitter.summingbird.storm

import com.twitter.summingbird.builder.{ FlatMapOption, SinkOption }
import com.twitter.summingbird.util.CacheSize

/**
  * intra-graph options.
  */

class StormOptions(opts: Map[Class[_], Any] = Map.empty) {
  def set(opt: SinkOption) = new StormOptions(opts + (opt.getClass -> opt))
  def set(opt: FlatMapOption) = new StormOptions(opts + (opt.getClass -> opt))
  def set(opt: CacheSize) = new StormOptions(opts + (opt.getClass -> opt))

  def get[T](klass: Class[T]): Option[T] =
    opts.get(klass).asInstanceOf[Option[T]]

  def getOrElse[T](klass: Class[T], default: T): T =
    opts.getOrElse(klass, default).asInstanceOf[T]

  private def klass[T: Manifest] = manifest[T].erasure.asInstanceOf[Class[T]]

  def get[T: Manifest]: Option[T] = get(klass[T])
  def getOrElse[T: Manifest](default: T): T = getOrElse(klass[T], default)
}
