package com.twitter.summingbird.storm.builder

import com.twitter.summingbird.option.JobId
import org.apache.storm.topology.IRichBolt
import org.apache.storm.tuple.Fields

private[builder] object BoltBuilder {
  def apply[I, O](
    jobId: JobId,
    boltId: Topology.BoltId[I, O],
    bolt: Topology.Bolt[I, O],
    inputEdges: List[Topology.Edge[I]],
    outputEdges: List[Topology.Edge[O]]
  ): IRichBolt = {
    val fields = outputEdges.headOption.map(_.edgeType.fields).getOrElse(new Fields())
    assert(
      outputEdges.forall(_.edgeType.fields == fields),
      "Output edges should have same `Fields` object")
    val outputInjection = outputEdges.headOption.map(_.edgeType.injection).orNull
    assert(
      outputEdges.forall(_.edgeType.injection == outputInjection),
      "Output edges should have same `Fields` object")

    BaseBolt(
      jobId,
      boltId,
      bolt.metrics,
      bolt.anchorTuples,
      hasDependants = outputEdges.nonEmpty,
      bolt.ackOnEntry,
      bolt.maxExecutePerSec,
      inputEdges.map(edge => (edge.dest.id, edge.edgeType.injection)).toMap,
      fields,
      outputInjection,
      bolt.executor
    )
  }
}
