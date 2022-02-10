package com.twitter.summingbird.online.executor

import com.twitter.conversions.time.longToTimeableNumber
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, MaxFutureWaitTime, MaxWaitingFutures }
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.{ Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Scope, State }
import org.openjdk.jmh.infra.Blackhole

/**
 * Command to run (should be executed from sbt with project summingbird-online-perf):
 * jmh:run -i 10 -wi 10 -f1 -t1 ComposedFlatMapWithTimePerformance
 *
 * Benchmark                                                                            Mode  Cnt     Score    Error  Units
 * ComposedFlatMapWithTimePerformance.measureDirectComplex                              avgt   10   340.341 ±  4.343  ms/op
 * ComposedFlatMapWithTimePerformance.measureDirectOperationContainer                   avgt   10   386.593 ±  6.474  ms/op
 * ComposedFlatMapWithTimePerformance.measureDirectSimple                               avgt   10   279.628 ±  6.175  ms/op
 * ComposedFlatMapWithTimePerformance.measureOperationContainerComposeThroughOperation  avgt   10  2978.141 ± 75.213  ms/op
 * ComposedFlatMapWithTimePerformance.measureOperationContainerDirectCompose            avgt   10  1198.601 ± 54.871  ms/op
 */
@State(Scope.Thread)
class ComposedFlatMapWithTimePerformance {
  val size = 1000000
  val input = (0 to size).map(i => (Timestamp(0), i)).toArray

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
  val composedOp: FlatMapOperation[Int, Int] =
    FlatMapOperation.apply(f1).flatMap(f2).flatMap(f3)

  val composedWithTimeSimple: ((Timestamp, Int)) => TraversableOnce[(Timestamp, Int)] =
    withTime(composed)
  val composedWithTimeComplex: ((Timestamp, Int)) => TraversableOnce[(Timestamp, Int)] =
    flatMap(
      flatMap(withTime(f1), withTime(f2)),
      withTime(f3)
    )
  val composedContainerDirect: OperationContainer[(Timestamp, Int), (Timestamp, Int), InputState[Int]] =
    new SimpleFlatMap(composedWithTimeComplex)
  val composedContainerComposeThroughOperation:
    OperationContainer[(Timestamp, Int), (Timestamp, Int), InputState[Int]] = new IntermediateFlatMap(
      withTimeOp(composedOp),
      MaxWaitingFutures(1000),
      MaxFutureWaitTime(1000.millis),
      MaxEmitPerExecute(Int.MaxValue)
    )
  val composedContainerDirectCompose:
    OperationContainer[(Timestamp, Int), (Timestamp, Int), InputState[Int]] = new IntermediateFlatMap(
    FlatMapOperation.apply(composedWithTimeComplex),
    MaxWaitingFutures(1000),
    MaxFutureWaitTime(1000.millis),
    MaxEmitPerExecute(Int.MaxValue)
  )

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureDirectSimple(bh: Blackhole): Unit =
    input.foreach(composedWithTimeSimple.apply(_).foreach(bh.consume))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureDirectComplex(bh: Blackhole): Unit =
    input.foreach(composedWithTimeComplex.apply(_).foreach(bh.consume))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureDirectOperationContainer(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(composedContainerDirect, input, bh)

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureOperationContainerComposeThroughOperation(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(composedContainerComposeThroughOperation, input, bh)

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureOperationContainerDirectCompose(bh: Blackhole): Unit =
    TestUtils.testOperationContainer(composedContainerDirectCompose, input, bh)

  def flatMap[A, B, C](
    f1: A => TraversableOnce[B],
    f2: B => TraversableOnce[C]
  ): (A => TraversableOnce[C]) =
    x => f1(x).flatMap(f2)

  def withTime[A, B](f: A => TraversableOnce[B]): ((Timestamp, A)) => TraversableOnce[(Timestamp, B)] = {
    case (timestamp, v) => f.apply(v).map((timestamp, _))
  }

  def withTimeOp[A, B](f: FlatMapOperation[A, B]): FlatMapOperation[(Timestamp, A), (Timestamp, B)] =
    FlatMapOperation.generic { case (timestamp, v) =>
      f.apply(v).map(elements => elements.map((timestamp, _)))
    }
}
