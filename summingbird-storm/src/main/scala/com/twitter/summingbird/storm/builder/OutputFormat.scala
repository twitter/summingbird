package com.twitter.summingbird.storm.builder

import com.twitter.bijection.Injection
import java.util.{ArrayList => JArrayList, List => JList}
import org.apache.storm.tuple.Fields
import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer
import scala.util.{ Failure, Try }

private[storm] case class OutputFormat[T](fields: List[String], injection: Injection[T, JList[AnyRef]]) {
  def asStormFields: Fields = new Fields(ListBuffer(fields: _*).asJava)
}

private[storm] object OutputFormat {
  def nullFormat[T]: OutputFormat[T] = OutputFormat[T](List(), new Injection[T, JList[AnyRef]] {
    override def apply(a: T): JList[AnyRef] = new JArrayList()
    override def invert(b: JList[AnyRef]): Try[T] =
      Failure(new Exception("cannot invert nullFormat"))
  })

  def get[T](outgoingEdges: Traversable[Topology.Edge[T, _]]): Option[OutputFormat[T]] = {
    if (outgoingEdges.isEmpty) None else {
      val format = outgoingEdges.head.format
      assert(
        outgoingEdges.forall(edge => edge.format == format),
        s"Outgoing edges should have same format $outgoingEdges")
      Some(format)
    }
  }
}
