package com.twitter.summingbird.online.executor

import chain.Chain
import scala.util.Try

private[executor] class SimpleFlatMap[I, O, S](f: I => TraversableOnce[O])
  extends OperationContainer[I, O, S] {

  override def executeTick: TraversableOnce[(Chain[S], Try[TraversableOnce[O]])] =
    None

  override def execute(state: S,data: I): TraversableOnce[(Chain[S], Try[TraversableOnce[O]])] =
    Some((Chain.single(state), Try(f(data))))
}
