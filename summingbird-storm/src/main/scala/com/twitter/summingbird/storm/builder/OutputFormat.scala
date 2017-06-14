package com.twitter.summingbird.storm.builder

import com.twitter.bijection.Injection
import org.apache.storm.tuple.Fields
import java.util.{ List => JList }

private[builder] case class OutputFormat[T](fields: Fields, injection: Injection[T, JList[AnyRef]])

private[builder] object OutputFormat {
  def get[T](outgoingEdges: Traversable[Topology.Edge[T, _]]): Option[OutputFormat[T]] = {
    if (outgoingEdges.isEmpty) None else {
      val fields = outgoingEdges.head.edgeType.fields
      assert(
        outgoingEdges.forall(edge => edge.edgeType.fields.toList == fields.toList),
        s"Outgoing edges should have same `Fields` $outgoingEdges")
      val injection = outgoingEdges.head.edgeType.injection
      assert(
        outgoingEdges.forall(edge => edge.edgeType.injection == injection),
        s"Outgoing edges should have same `Injection` $outgoingEdges")
      Some(OutputFormat(fields, injection))
    }
  }
}
