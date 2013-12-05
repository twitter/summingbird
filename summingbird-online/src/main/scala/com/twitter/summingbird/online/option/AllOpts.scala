package com.twitter.summingbird.online.option
import com.twitter.util.Duration

case class OnlineSuccessHandler(handlerFn: Unit => Unit)


/**
  * Kryo serialization problems have been observed with using
  * OnlineSuccessHandler. This enables easy disabling of the handler.
  * TODO (https://github.com/twitter/summingbird/issues/82): remove
  * once we know what the hell is going on with this
  */
case class IncludeSuccessHandler(get: Boolean)

object IncludeSuccessHandler {
  val default = IncludeSuccessHandler(true)
}

case class OnlineExceptionHandler(handlerFn: PartialFunction[Throwable, Unit])

/**
  * MaxWaitingFutures is the maximum number of key-value pairs that the
  * SinkBolt in Storm will process before starting to force the
  * futures. For example, setting MaxWaitingFutures(100) means that if
  * a key-value pair is added to the OnlineStore and the (n - 100)th
  * write has not completed, Storm will block before moving on to the
  * next key-value pair.

  * TODO (https://github.com/twitter/summingbird/issues/83): look into
  * removing this due to the possibility of deadlock with the sink's
  * cache.
  */
case class MaxWaitingFutures(get: Int)

/**
 * All futures should return in a reasonable period of time, otherwise
 * there will be memory issues keeping all of them open. This option is
 * to set the longest we wait on a future. It is not a substitute for correctly
 * configured and implemented stores, services and sinks. All of those should
 * return or fail fairly quickly (on the order of a second or so).
 */
case class MaxFutureWaitTime(get: Duration)

/*
  FlushFrequency is how often regardless of traffic a given Cache should be flushed to the network.
*/
case class FlushFrequency(get: Duration)

/*
 UseAsyncCache is used to enable a background asynchronous cache. These do all cache related operations in background threads.
 */
case class UseAsyncCache(get: Boolean)

/*
  AsyncPoolSize controls the size of the fixed thread pool used to back an asynchronous cache.
  Only will have an effect if UseAsyncCache is true
*/
case class AsyncPoolSize(get: Int)

/*
 SoftMemoryFlushPercent is the percentage of memory used in the JVM at which a flush will be triggered of the cache.
*/
case class SoftMemoryFlushPercent(get: Float) {
  require(0 < get && get <= 100.0, "must be a percentage.")
}

/*
  ValueCombinerCacheSize is used in cache's that support it as a trigger to crush down a high locality of values without emitting
*/
case class ValueCombinerCacheSize(get: Int)