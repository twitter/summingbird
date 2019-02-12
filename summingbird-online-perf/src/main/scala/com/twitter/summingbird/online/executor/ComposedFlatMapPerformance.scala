package com.twitter.summingbird.online.executor

import com.twitter.conversions.time.longToTimeableNumber
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, MaxFutureWaitTime, MaxWaitingFutures }
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.{ Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Scope, State }
import org.openjdk.jmh.infra.Blackhole

/**
 * Command to run (should be executed from sbt with project summingbird-online-perf):
 * jmh:run -i 10 -wi 10 -f1 -t1 ComposedFlatMapPerformance
 *
 * Benchmark                                                                    Mode  Cnt     Score     Error  Units
 * ComposedFlatMapPerformance.measureDirect                                     avgt   10   239.003 ±  19.990  ms/op
 * ComposedFlatMapPerformance.measureDirectOperationContainer                   avgt   10   295.847 ±  14.774  ms/op
 * ComposedFlatMapPerformance.measureOperationContainerComposeThroughOperation  avgt   10  2733.537 ±  63.415  ms/op
 * ComposedFlatMapPerformance.measureOperationContainerDirectCompose            avgt   10  1084.457 ±  22.968  ms/op
 */
@State(Scope.Thread)
class ComposedFlatMapPerformance {
  val size = 1000000
  val input = Array.range(0, size)

  val f1: (Int => TraversableOnce[Int]) = x => Some(x * 2)
  val f2: (Int => TraversableOnce[(Int, Int)]) = x => Some((x, x))
  val f3: (((Int, Int)) => TraversableOnce[Int]) = pair => List(
    pair._1,
    pair._2,
    pair._1 + pair._2,
    pair._1 * pair._2
  )

  val composed: (Int) => TraversableOnce[Int] =
    flatMap[Int, (Int, Int), Int](flatMap[Int, Int, (Int, Int)](f1, f2), f3)
  val composedContainerDirect: OperationContainer[Int, Int, InputState[Int]] =
    new SimpleFlatMap(composed)
  val composedContainer: OperationContainer[Int, Int, InputState[Int]] =
    new IntermediateFlatMap(
      FlatMapOperation.apply(f1).flatMap(f2).flatMap(f3),
      MaxWaitingFutures(1000),
      MaxFutureWaitTime(1000.millis),
      MaxEmitPerExecute(Int.MaxValue)
    )
  val directlyComposedContainer: OperationContainer[Int, Int, InputState[Int]] =
    new IntermediateFlatMap(
      FlatMapOperation.apply(composed),
      MaxWaitingFutures(1000),
      MaxFutureWaitTime(1000.millis),
      MaxEmitPerExecute(Int.MaxValue)
    )

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureDirect(bh: Blackhole): Unit =
    input.foreach(composed.apply(_).foreach(bh.consume))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureDirectOperationContainer(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(composedContainerDirect, input, bh)

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureOperationContainerComposeThroughOperation(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(composedContainer, input, bh)

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureOperationContainerDirectCompose(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(directlyComposedContainer, input, bh)

  def flatMap[A, B, C](
    f1: A => TraversableOnce[B],
    f2: B => TraversableOnce[C]
  ): (A => TraversableOnce[C]) =
    x => f1(x).flatMap(f2)
}
