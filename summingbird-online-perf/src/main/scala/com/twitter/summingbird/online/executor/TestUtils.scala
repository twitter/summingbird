package com.twitter.summingbird.online.executor

import org.openjdk.jmh.infra.Blackhole
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object TestUtils {
  val TickDelay = 5.millis

  def testOperationContainer[I, O](
    container: OperationContainer[I, O, InputState[Int]],
    inputs: Array[I],
    bh: Blackhole
  ): Unit = {
    var left = inputs.length
    for (
      input <- inputs;
      emitted <- container.execute(InputState[Int](1), input)
    ) {
      emitted match {
        case (states, output) =>
          states.foreach { _ => left -= 1 }
          output match {
            case Success(value) => value.foreach(bh.consume)
            case Failure(exception) => bh.consume(exception)
          }
      }
    }
    while (left != 0) {
      Thread.sleep(TickDelay.toMillis)
      for (emitted <- container.executeTick) {
        emitted match {
          case (states, output) =>
            states.foreach { _ => left -= 1 }
            output match {
              case Success(value) => value.foreach(bh.consume)
              case Failure(exception) => bh.consume(exception)
            }
        }
      }
    }
  }
}
