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
    val outputFields = outputEdges.headOption.map(_.edgeType.fields).getOrElse(new Fields())
    assert(
      outputEdges.forall(_.edgeType.fields.toList == outputFields.toList),
      s"$boltId: Output edges should have same `Fields` $outputEdges")
    val outputInjection = outputEdges.headOption.map(_.edgeType.injection).orNull
    assert(
      outputEdges.forall(_.edgeType.injection == outputInjection),
      s"$boltId: Output edges should have same `Injection` $outputEdges.")

    BaseBolt(
      jobId,
      boltId,
      bolt.metrics,
      bolt.anchorTuples,
      hasDependants = outputEdges.nonEmpty,
      bolt.ackOnEntry,
      bolt.maxExecutePerSec,
      inputEdges.map(edge => (edge.source.id, edge.edgeType.injection)).toMap,
      outputFields,
      outputInjection,
      bolt.executor
    )
  }
}
