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

  case object Null extends SummerWithCountersBuilder {
    override def create(counter: (Name) => Incrementor): SummerBuilder = {
      val tuplesIn = counter(TuplesInCounterName)
      val tuplesOut = counter(TuplesOutCounterName)
      new SummerBuilder {
        override def getSummer[K, V: Semigroup]: AsyncSummer[(K, V), Map[K, V]] =
          new com.twitter.algebird.util.summer.NullSummer[K, V](tuplesIn, tuplesOut)
      }
    }
  }

  case class Sync(
    cacheSize: CacheSize = DEFAULT_FM_CACHE,
    flushFrequency: FlushFrequency = DEFAULT_FLUSH_FREQUENCY,
    softMemoryFlushPercent: SoftMemoryFlushPercent = DEFAULT_SOFT_MEMORY_FLUSH_PERCENT
  ) extends SummerWithCountersBuilder {
    override def create(counter: (Name) => Incrementor): SummerBuilder = {
      val memoryCounter = counter(MemoryCounterName)
      val timeoutCounter = counter(TimeoutCounterName)
      val sizeCounter = counter(SizeCounterName)
      val tupleInCounter = counter(TuplesInCounterName)
      val tupleOutCounter = counter(TuplesOutCounterName)
      val insertCounter = counter(InsertCounterName)

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

  case class Async(
    cacheSize: CacheSize = DEFAULT_FM_CACHE,
    flushFrequency: FlushFrequency = DEFAULT_FLUSH_FREQUENCY,
    softMemoryFlushPercent: SoftMemoryFlushPercent = DEFAULT_SOFT_MEMORY_FLUSH_PERCENT,
    asyncPoolSize: AsyncPoolSize = DEFAULT_ASYNC_POOL_SIZE,
    compactValues: CompactValues = CompactValues.default,
    valueCombinerCacheSize: ValueCombinerCacheSize = DEFAULT_VALUE_COMBINER_CACHE_SIZE
  ) extends SummerWithCountersBuilder {
    override def create(counter: (Name) => Incrementor): SummerBuilder = {
      val memoryCounter = counter(MemoryCounterName)
      val timeoutCounter = counter(TimeoutCounterName)
      val sizeCounter = counter(SizeCounterName)
      val tupleInCounter = counter(TuplesInCounterName)
      val tupleOutCounter = counter(TuplesOutCounterName)
      val insertCounter = counter(InsertCounterName)
      val insertFailCounter = counter(InsertFailCounterName)

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
