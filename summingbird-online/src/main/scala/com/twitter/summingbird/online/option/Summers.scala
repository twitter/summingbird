package com.twitter.summingbird.online.option

import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer._
import com.twitter.summingbird.Name
import com.twitter.summingbird.online.OnlineDefaultConstants._
import com.twitter.summingbird.option.CacheSize
import com.twitter.util.{ Future, FuturePool }
import java.util.concurrent.{ Executors, TimeUnit }

object Summers {
  val MemoryCounterName = Name("memory")
  val TimeoutCounterName = Name("timeout")
  val SizeCounterName = Name("size")
  val TuplesInCounterName = Name("tuplesIn")
  val TuplesOutCounterName = Name("tuplesOut")
  val InsertCounterName = Name("inserts")
  val InsertFailCounterName = Name("insertFail")

  val Null = new SummerConstructor(NullConstructor)

  def sync(
    cacheSize: CacheSize = DEFAULT_FM_CACHE,
    flushFrequency: FlushFrequency = DEFAULT_FLUSH_FREQUENCY,
    softMemoryFlushPercent: SoftMemoryFlushPercent = DEFAULT_SOFT_MEMORY_FLUSH_PERCENT
  ): SummerConstructor = new SummerConstructor(SyncConstructor(cacheSize, flushFrequency, softMemoryFlushPercent))

  def async(
    cacheSize: CacheSize = DEFAULT_FM_CACHE,
    flushFrequency: FlushFrequency = DEFAULT_FLUSH_FREQUENCY,
    softMemoryFlushPercent: SoftMemoryFlushPercent = DEFAULT_SOFT_MEMORY_FLUSH_PERCENT,
    asyncPoolSize: AsyncPoolSize = DEFAULT_ASYNC_POOL_SIZE,
    compactValues: CompactValues = CompactValues.default,
    valueCombinerCacheSize: ValueCombinerCacheSize = DEFAULT_VALUE_COMBINER_CACHE_SIZE
  ): SummerConstructor = new SummerConstructor(AsyncConstructor(
    cacheSize, flushFrequency, softMemoryFlushPercent, asyncPoolSize, compactValues, valueCombinerCacheSize
  ))

  private object NullConstructor extends (SummerConstructor.Context => SummerBuilder) {
    override def apply(ctx: SummerConstructor.Context): SummerBuilder = {
      val tuplesIn = ctx.counter(TuplesInCounterName)
      val tuplesOut = ctx.counter(TuplesOutCounterName)
      new SummerBuilder {
        override def getSummer[K, V: Semigroup]: AsyncSummer[(K, V), Map[K, V]] =
          new com.twitter.algebird.util.summer.NullSummer[K, V](tuplesIn, tuplesOut)
      }
    }
  }

  private case class SyncConstructor(
    cacheSize: CacheSize,
    flushFrequency: FlushFrequency,
    softMemoryFlushPercent: SoftMemoryFlushPercent
  ) extends (SummerConstructor.Context => SummerBuilder) {
    override def apply(ctx: SummerConstructor.Context): SummerBuilder = {
      val memoryCounter = ctx.counter(MemoryCounterName)
      val timeoutCounter = ctx.counter(TimeoutCounterName)
      val sizeCounter = ctx.counter(SizeCounterName)
      val tupleInCounter = ctx.counter(TuplesInCounterName)
      val tupleOutCounter = ctx.counter(TuplesOutCounterName)
      val insertCounter = ctx.counter(InsertCounterName)

      new SummerBuilder {
        def getSummer[K, V: Semigroup]: com.twitter.algebird.util.summer.AsyncSummer[(K, V), Map[K, V]] = {
          new SyncSummingQueue[K, V](
            BufferSize(cacheSize.lowerBound),
            com.twitter.algebird.util.summer.FlushFrequency(flushFrequency.get),
            MemoryFlushPercent(softMemoryFlushPercent.get),
            memoryCounter,
            timeoutCounter,
            sizeCounter,
            insertCounter,
            tupleInCounter,
            tupleOutCounter)
        }
      }
    }
  }

  private case class AsyncConstructor(
    cacheSize: CacheSize,
    flushFrequency: FlushFrequency,
    softMemoryFlushPercent: SoftMemoryFlushPercent,
    asyncPoolSize: AsyncPoolSize,
    compactValues: CompactValues,
    valueCombinerCacheSize: ValueCombinerCacheSize
  ) extends (SummerConstructor.Context => SummerBuilder) {
    override def apply(ctx: SummerConstructor.Context): SummerBuilder = {
      val memoryCounter = ctx.counter(MemoryCounterName)
      val timeoutCounter = ctx.counter(TimeoutCounterName)
      val sizeCounter = ctx.counter(SizeCounterName)
      val tupleInCounter = ctx.counter(TuplesInCounterName)
      val tupleOutCounter = ctx.counter(TuplesOutCounterName)
      val insertCounter = ctx.counter(InsertCounterName)
      val insertFailCounter = ctx.counter(InsertFailCounterName)

      new SummerBuilder {
        def getSummer[K, V: Semigroup]: com.twitter.algebird.util.summer.AsyncSummer[(K, V), Map[K, V]] = {
          val executor = Executors.newFixedThreadPool(asyncPoolSize.get)
          val futurePool = FuturePool(executor)
          val summer = new AsyncListSum[K, V](BufferSize(cacheSize.lowerBound),
            com.twitter.algebird.util.summer.FlushFrequency(flushFrequency.get),
            MemoryFlushPercent(softMemoryFlushPercent.get),
            memoryCounter,
            timeoutCounter,
            insertCounter,
            insertFailCounter,
            sizeCounter,
            tupleInCounter,
            tupleOutCounter,
            futurePool,
            Compact(compactValues.toBoolean),
            CompactionSize(valueCombinerCacheSize.get))
          summer.withCleanup(() => {
            Future {
              executor.shutdown
              executor.awaitTermination(10, TimeUnit.SECONDS)
            }
          })
        }
      }
    }
  }
}
