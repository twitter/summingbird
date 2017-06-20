package com.twitter.summingbird.storm.builder

import com.twitter.bijection.Injection
import java.util.{ List => JList }
import org.apache.storm.tuple.Fields
import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer

/**
 * Trait to specify storm's format for emitted values.
 * Contains storm's tuple's fields and injection from [[T]] to
 * storm's [[org.apache.storm.tuple.Values]] (which are java arrays).
 */
private[storm] sealed case class OutputFormat[T](
  fields: List[String],
  injection: Injection[T, JList[AnyRef]]
) {
  def asStormFields: Fields = new Fields(ListBuffer(fields: _*).asJava)
}

private[storm] object OutputFormat {
  def get[T](outgoingEdges: Iterable[Topology.Edge[T, _]]): Option[OutputFormat[T]] =
    outgoingEdges.headOption.map { head =>
      assert(
        outgoingEdges.forall(edge => edge.format == head.format),
        s"Outgoing edges should have same format $outgoingEdges")
      head.format
    }
}
