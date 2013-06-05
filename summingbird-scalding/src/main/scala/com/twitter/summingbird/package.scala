package com.twitter.summingbird

import com.twitter.scalding.TypedPipe

package object scalding {
  // which that key had been aggregated.
  type TimedPipe[+T] = TypedPipe[(Long, T)]
  type KeyValuePipe[+K, +V] = TimedPipe[(K, V)]
  type PipeFactory[+T] = BatchProducer[TimedPipe[T]]
}
