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

import com.twitter.summingbird._
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.OnlineServiceFactory

/**
 * A utility for converting a series of producers into a single FlatMapOperation
 * This simply folds through a list of producers converting them into an operation from T => Future[TraversableOnce[U]].
 * This allows us to combine simple map, flatmap with also left joins.
 *
 * This is platform specific, as the contents of what are in Producer's are also platform specific.
 */
object Producer2FlatMapOperation {
  /**
   * Keep the crazy casts localized in here
   */
  def foldOperations[T, U](producers: List[Producer[Storm, _]]): FlatMapOperation[T, U] =
    producers.foldLeft(FlatMapOperation.identity[Any]) {
      case (acc, p) =>
        p match {
          case LeftJoinedProducer(_, wrapper) =>
            FlatMapOperation.combine(
              acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
              wrapper.asInstanceOf[OnlineServiceFactory[Any, Any]]).asInstanceOf[FlatMapOperation[Any, Any]]
          case OptionMappedProducer(_, op) => acc.andThen(FlatMapOperation[Any, Any](op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]]))
          case FlatMappedProducer(_, op) => acc.andThen(FlatMapOperation(op).asInstanceOf[FlatMapOperation[Any, Any]])
          case WrittenProducer(_, sinkSupplier) =>
            acc.andThen(FlatMapOperation.write(() => sinkSupplier.toFn))
          case IdentityKeyedProducer(_) => acc
          case MergedProducer(_, _) => acc
          case NamedProducer(_, _) => acc
          case AlsoProducer(_, _) => acc
          case Source(_) => sys.error("Should not schedule a source inside a flat mapper")
          case Summer(_, _, _) => sys.error("Should not schedule a Summer inside a flat mapper")
          case KeyFlatMappedProducer(_, op) => acc.andThen(FlatMapOperation.keyFlatMap[Any, Any, Any](op).asInstanceOf[FlatMapOperation[Any, Any]])
        }
    }.asInstanceOf[FlatMapOperation[T, U]]
}
