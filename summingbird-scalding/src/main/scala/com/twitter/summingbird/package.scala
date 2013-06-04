package com.twitter.summingbird

import com.twitter.scalding.TypedPipe

package object scalding {
  type PipeFactory[+T] = (BatchID, BatchID) => TypedPipe[T]
}
