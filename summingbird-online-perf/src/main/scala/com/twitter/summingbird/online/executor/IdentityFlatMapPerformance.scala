package com.twitter.summingbird.online.executor

import com.twitter.conversions.time.longToTimeableNumber
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.option.{MaxEmitPerExecute, MaxFutureWaitTime, MaxWaitingFutures}
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Scope, State}
import org.openjdk.jmh.infra.Blackhole

/**
 * Command to run (should be executed from sbt with project summingbird-online-perf):
 * jmh:run -i 10 -wi 10 -f1 -t1 IdentityFlatMapPerformance
 *
 * Benchmark                                                   Mode  Cnt    Score    Error  Units
 * IdentityFlatMapPerformance.measureDirect                    avgt   10   42.184 ±  3.518  ms/op
 * IdentityFlatMapPerformance.measureDirectOperationContainer  avgt   10  101.944 ±  3.956  ms/op
 * IdentityFlatMapPerformance.measureOperationContainer        avgt   10  817.659 ± 36.591  ms/op
 */
@State(Scope.Thread)
class IdentityFlatMapPerformance {
  val size = 1000000
  val input = Array.range(0, size)

  val f: (Int => TraversableOnce[Int]) = x => Iterator(x)
  val containerDirect: OperationContainer[Int, Int, InputState[Int]] =
    new SimpleFlatMap(f)
  val container: OperationContainer[Int, Int, InputState[Int]] =
    new IntermediateFlatMap(
      FlatMapOperation.apply(f),
      MaxWaitingFutures(1000),
      MaxFutureWaitTime(1000.millis),
      MaxEmitPerExecute(Int.MaxValue)
    )

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureDirect(bh: Blackhole): Unit =
    input.foreach(f.apply(_).foreach(bh.consume))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureDirectOperationContainer(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(containerDirect, input, bh)

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureOperationContainer(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(container, input, bh)
}
