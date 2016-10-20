package com.twitter.summingbird.storm

import backtype.storm.spout.ISpoutOutputCollector
import backtype.storm.tuple.Values
import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.{ AsyncSummer, BufferSize, FlushFrequency, MemoryFlushPercent, SyncSummingQueue }
import com.twitter.summingbird.online.executor.KeyValueShards
import com.twitter.summingbird.online.option.{ SummerBuilder, MaxEmitPerExecute }
import com.twitter.summingbird.storm.spout.KeyValueSpout
import com.twitter.tormenta.spout.{ BaseSpout, Spout }
import com.twitter.util.Duration
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.storm.collector.AggregatorOutputCollector
import java.util.{ List => JList }
import org.scalatest.WordSpec
import org.scalacheck._
import scala.collection.mutable.{ Set => MSet }

/**
 * Created by pnaramsetti on 8/18/16.
 */

object TestKeyValueSpout {
  def getSyncSummingQueueBuildSummer(batchSize: Int, flushFrequency: Duration, memFlushPercent: Int) = {
    new SummerBuilder {
      def getSummer[K, V: Semigroup]: AsyncSummer[(K, V), Map[K, V]] = {
        new SyncSummingQueue[K, V](
          BufferSize(batchSize),
          FlushFrequency(flushFrequency),
          MemoryFlushPercent(memFlushPercent),
          Counter("memory"),
          Counter("timeout"),
          Counter("size"),
          Counter("insert"),
          Counter("tupleIn"),
          Counter("tupleOut"))
      }
    }
  }
}
class TestKeyValueSpout extends WordSpec {

  def process(spout: Spout[(Timestamp, (Int, Int))], summer: SummerBuilder, expected: MSet[TestAggregateOutpoutCollector.ExpectedTuple]) = {
    val formattedSummerSpout = spout.map {
      case (time, (k, v)) => ((k, BatchID(1)), (time, v))
    }
    val flushCounter = Counter("flushTime")
    val execCounter = Counter("execTime")
    val outputCollector: ISpoutOutputCollector = new MockedISpoutOutputCollector
    val testSpout = new KeyValueSpout[(Int, BatchID), (Timestamp, Int)](
      formattedSummerSpout.getSpout,
      summer,
      MaxEmitPerExecute(10),
      KeyValueShards(1),
      flushCounter,
      execCounter
    )
    val myCollector = new TestAggregateOutpoutCollector(outputCollector, expected)
    testSpout.open(null, null, myCollector)
    (testSpout, myCollector)
  }

  "Check two batches are being sent - 4 different tuples" in {
    val summer = TestKeyValueSpout.getSyncSummingQueueBuildSummer(1, Duration.fromSeconds(1), 1)
    val timeStamp = Timestamp.now

    //input
    val basespout = new BaseSpout[(Timestamp, (Int, Int))] {
      override def poll = scala.collection.immutable.List((timeStamp, (1, 1)), (timeStamp, (2, 1)), (timeStamp, (3, 1)), (timeStamp, (4, 1))).toSeq
    }

    //output
    val expectedTuples = TestAggregateOutpoutCollector.emptyTupleSet
    expectedTuples.add((0, Map((1, BatchID(1)) -> ((timeStamp, 1)), (2, BatchID(1)) -> ((timeStamp, 1))), None, None))
    expectedTuples.add((0, Map((3, BatchID(1)) -> ((timeStamp, 1)), (4, BatchID(1)) -> ((timeStamp, 1))), None, None))

    val (spout, collector) = process(basespout, summer, expectedTuples)
    spout.nextTuple()
    assert(collector.getSize == 0)
  }

  "Tuples are to be aggregated - all four tuples should be crushed down" in {
    val summer = TestKeyValueSpout.getSyncSummingQueueBuildSummer(2, Duration.fromSeconds(1), 1)
    val timeStamp = Timestamp.now

    //input
    val basespout = new BaseSpout[(Timestamp, (Int, Int))] {
      override def poll = scala.collection.immutable.List((timeStamp, (1, 1)), (timeStamp, (1, 2)), (timeStamp, (1, 3)), (timeStamp, (1, 4))).toSeq
    }

    //output
    val expectedTuples = TestAggregateOutpoutCollector.emptyTupleSet
    expectedTuples.add((0, Map((1, BatchID(1)) -> ((timeStamp, 6))), None, None))

    val (spout, collector) = process(basespout, summer, expectedTuples)
    spout.nextTuple()
    assert(collector.getSize == 0)
  }

  "No tuples should be emitted without timerflush - batch size greater than number of tuples" in {
    val summer = TestKeyValueSpout.getSyncSummingQueueBuildSummer(10, Duration.fromSeconds(1), 1)
    val timeStamp = Timestamp.now

    //input
    val basespout = new BaseSpout[(Timestamp, (Int, Int))] {
      override def poll = scala.collection.immutable.List((timeStamp, (1, 1)), (timeStamp, (2, 1)), (timeStamp, (3, 1)), (timeStamp, (4, 1))).toSeq
    }

    //output
    val expectedTuples = TestAggregateOutpoutCollector.emptyTupleSet

    val (spout, collector) = process(basespout, summer, expectedTuples)
    spout.nextTuple()
    assert(collector.getSize == 0)
  }

  "One batch should be sent on timerFlush - batch size greater than number of tuples" in {
    val summer = TestKeyValueSpout.getSyncSummingQueueBuildSummer(10, Duration.fromSeconds(1), 1)
    val timeStamp = Timestamp.now

    //input
    val basespout = new BaseSpout[(Timestamp, (Int, Int))] {
      override def poll = scala.collection.immutable.List((timeStamp, (1, 1)), (timeStamp, (2, 1)), (timeStamp, (3, 1)), (timeStamp, (4, 1)))
    }

    //output
    val expectedTuples = TestAggregateOutpoutCollector.emptyTupleSet
    expectedTuples.add((0, Map((2, BatchID(1)) -> ((timeStamp, 1)), (1, BatchID(1)) -> ((timeStamp, 1)), (3, BatchID(1)) -> ((timeStamp, 1)), (4, BatchID(1)) -> ((timeStamp, 1))), None, None))

    val (spout, collector) = process(basespout, summer, expectedTuples)
    spout.nextTuple()
    Thread.sleep(1200)
    spout.nextTuple()
    assert(collector.getSize == 0)
  }

}

class MockedISpoutOutputCollector extends ISpoutOutputCollector {
  override def reportError(throwable: Throwable): Unit = ???

  override def emitDirect(
    i: Int,
    s: String,
    list: JList[AnyRef],
    o: scala.Any): Unit = ???

  override def emit(
    s: String,
    list: JList[AnyRef],
    o: scala.Any): JList[Integer] = ???
}
