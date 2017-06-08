package com.twitter.summingbird.storm.builder

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.option.JobId
import EdgeType.AggregatedKeyValues
import com.twitter.summingbird.{ Group, JobCounters, Name, SummingbirdRuntimeStats }
import com.twitter.summingbird.storm._
import com.twitter.tormenta.spout.Spout
import org.apache.storm.metric.api.IMetric
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import scala.collection.{ Map => CMap }

private[builder] object SpoutBuilder {
  def apply[T](
    jobId: JobId,
    spoutId: Topology.SpoutId[T],
    spout: Topology.Spout[T],
    outputEdges: List[Topology.Edge[T]]
  ): IRichSpout = spout match {
    case rawSpout: Topology.RawSpout[T] =>
      createRawSpout(jobId, spoutId, rawSpout, outputEdges)
    case keyValueSpout: Topology.KeyValueSpout[_, _] =>
      createKeyValueSpout[Any, Any](
        jobId,
        spoutId.asInstanceOf[Topology.SpoutId[(Int, CMap[Any, Any])]],
        keyValueSpout.asInstanceOf[Topology.KeyValueSpout[Any, Any]],
        outputEdges.asInstanceOf[List[Topology.Edge[(Int, CMap[Any, Any])]]]
      )(
        keyValueSpout.semigroup.asInstanceOf[Semigroup[Any]]
      )
  }

  def createRawSpout[T](
    jobId: JobId,
    spoutId: Topology.SpoutId[T],
    spout: Topology.RawSpout[T],
    outputEdges: List[Topology.Edge[T]]
  ): IRichSpout = {
    assert(
      outputEdges.forall(_.edgeType.isInstanceOf[EdgeType.Item[_]]),
      "Only `Item` edges supported from Tormenta Spout"
    )

    getStormSpout(jobId, spout.spout, spout.metrics)
  }

  def createKeyValueSpout[K, V: Semigroup](
    jobId: JobId,
    spoutId: Topology.SpoutId[(Int, CMap[K, V])],
    spout: Topology.KeyValueSpout[K, V],
    outputEdges: List[Topology.Edge[(Int, CMap[K, V])]]
  ): IRichSpout = {
    val stormSpout = getStormSpout(jobId, spout.spout, spout.metrics)
    assert(outputEdges.nonEmpty, "Should be at least one output edge from KeyValueSpout")
    assert(
      outputEdges.forall(edge => edge.edgeType.isInstanceOf[AggregatedKeyValues[K, V]]),
      "All output edges from KeyValueSpout should be `AggregatedKeyValues`"
    )
    val shards = outputEdges.head.edgeType.asInstanceOf[AggregatedKeyValues[K, V]].shards
    assert(
      outputEdges.forall(edge => edge.edgeType.asInstanceOf[AggregatedKeyValues[K, V]].shards == shards),
      "All output edges from KeyValueSpout should have the same shard number"
    )

    new KeyValueSpout[K, V](
      stormSpout,
      spout.summerBuilder,
      spout.maxEmitPerExec,
      shards,
      spout.flushExecTimeCounter,
      spout.executeTimeCounter
    )
  }

  // Tormenta bug when openHook might get lost if it doesn't happen right before the getSpout.
  private def getStormSpout[T](
    jobId: JobId,
    spout: Spout[T],
    metrics: () => TraversableOnce[StormMetric[_]]
  ): IRichSpout = {
    val register: Externalizer[(TopologyContext) => Unit] = createRegistrationFunction(jobId, metrics)
    spout.openHook(register.get).getSpout
  }

  private def getCountersForJob(jobId: JobId): Seq[(Group, Name)] =
    JobCounters.getCountersForJob(jobId).getOrElse(Nil)

  private def createRegistrationFunction(
    jobId: JobId,
    metrics: () => TraversableOnce[StormMetric[_]]
  ): Externalizer[(TopologyContext) => Unit] = {
    val counters: Seq[(Group, Name)] = getCountersForJob(jobId)
    Externalizer({ context: TopologyContext =>
      metrics().foreach {
        x: StormMetric[_] =>
          context.registerMetric(x.name, x.metric.asInstanceOf[IMetric], x.interval.inSeconds)
      }
      // Register summingbird counter metrics.
      StormStatProvider.registerMetrics(jobId, context, counters)
      SummingbirdRuntimeStats.addPlatformStatProvider(StormStatProvider)
    })
  }
}
