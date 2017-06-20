package com.twitter.summingbird.storm.builder

import com.twitter.tormenta.spout.SpoutProxy
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.{ IRichSpout, OutputFieldsDeclarer }
import java.util.{ List => JList, Map => JMap }
import org.apache.storm.testing.CompletableSpout

/**
 * [[IRichSpout]] wrapper which gives an ability to specify [[OutputFormat]].
 */
private[builder] class FormattedSpout[T](
  protected val self: IRichSpout,
  format: OutputFormat[T]
) extends SpoutProxy {
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit =
    declarer.declare(format.asStormFields)

  override def open(
    conf: JMap[_, _],
    topologyContext: TopologyContext,
    outputCollector: SpoutOutputCollector
  ): Unit =
    super.open(conf, topologyContext, new FormattedSpout.OutputCollector[T](outputCollector, format))
}

/**
 * Same as [[FormattedSpout]] but with [[CompletableSpout]] support, which is used for testing.
 */
private[builder] class CompletableFormattedSpout[T](
  inner: IRichSpout with CompletableSpout,
  format: OutputFormat[T]
) extends FormattedSpout[T](inner, format) with CompletableSpout {
  override def startup(): AnyRef = inner.startup()
  override def exhausted_QMARK_(): AnyRef = inner.exhausted_QMARK_()
  override def cleanup(): AnyRef = inner.cleanup()
}

private[builder] object FormattedSpout {
  case class OutputCollector[T](in: SpoutOutputCollector, format: OutputFormat[T])
    extends SpoutOutputCollector(in) {
    override def emitDirect(
      taskId: Int,
      streamId: String,
      tuple: JList[AnyRef],
      messageId: AnyRef
    ): Unit = in.emitDirect(taskId, streamId, transform(tuple), messageId)

    override def emit(
      streamId: String,
      tuple: JList[AnyRef],
      messageId: AnyRef
    ): JList[Integer] = in.emit(streamId, transform(tuple), messageId)

    private def transform(tuple: JList[AnyRef]): JList[AnyRef] = {
      // We assume incoming tuples are stored in a format of one element in list with actual value.
      assert(tuple.size() == 1)
      format.injection(tuple.get(0).asInstanceOf[T])
    }
  }
}
