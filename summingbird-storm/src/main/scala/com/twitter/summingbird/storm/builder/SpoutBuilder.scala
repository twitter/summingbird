package com.twitter.summingbird.storm.builder

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.online.Externalizer
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.{ Group, JobCounters, Name, SummingbirdRuntimeStats }
import com.twitter.summingbird.storm._
import com.twitter.tormenta.spout.{ SchemeSpout, Spout }
import org.apache.storm.metric.api.IMetric
import org.apache.storm.task.TopologyContext
import org.apache.storm.testing.CompletableSpout
import org.apache.storm.topology.IRichSpout
import scala.collection.{ Map => CMap }

private[builder] object SpoutBuilder {
  def build[O](
    jobId: JobId,
    spoutId: Topology.SpoutId[O],
    spout: Topology.Spout[O],
    outgoingEdges: List[Topology.Edge[O, _]]
  ): IRichSpout = spout match {
    case rawSpout: Topology.RawSpout[O] =>
      createRawSpout(jobId, spoutId, rawSpout, outgoingEdges)
    case keyValueSpout: Topology.KeyValueSpout[_, _] =>
      createKeyValueSpout[Any, Any](
        jobId,
        spoutId.asInstanceOf[Topology.SpoutId[(Int, CMap[Any, Any])]],
        keyValueSpout.asInstanceOf[Topology.KeyValueSpout[Any, Any]],
        outgoingEdges.asInstanceOf[List[Topology.Edge[(Int, CMap[Any, Any]), _]]]
      )(
        keyValueSpout.semigroup.asInstanceOf[Semigroup[Any]]
      )
  }

  def createRawSpout[O](
    jobId: JobId,
    spoutId: Topology.SpoutId[O],
    spout: Topology.RawSpout[O],
    outgoingEdges: List[Topology.Edge[O, _]]
  ): IRichSpout =
    getStormSpout(jobId, spout.spout, spout.metrics, OutputFormat.get(outgoingEdges))

  def createKeyValueSpout[K, V: Semigroup](
    jobId: JobId,
    spoutId: Topology.SpoutId[(Int, CMap[K, V])],
    spout: Topology.KeyValueSpout[K, V],
    outgoingEdges: List[Topology.Edge[(Int, CMap[K, V]), _]]
  ): IRichSpout = {
    assert(outgoingEdges.nonEmpty, "Should be at least one output edge from KeyValueSpout")
    // No need to apply custom output formats in case of KeyValue spout, we are going to wrap it later.
    val stormSpout = getStormSpout(jobId, spout.spout, spout.metrics, None)
    new KeyValueSpout[K, V](
      stormSpout,
      spout.summerBuilder,
      spout.maxEmitPerExec,
      spout.shards,
      spout.flushExecTimeCounter,
      spout.executeTimeCounter,
      OutputFormat.get(outgoingEdges).get
    )
  }

  private def getStormSpout[T](
    jobId: JobId,
    spout: Spout[T],
    metrics: () => TraversableOnce[StormMetric[_]],
    formatOpt: Option[OutputFormat[T]]
  ): IRichSpout = {
    // Tormenta bug when openHook might get lost if it doesn't happen right before the getSpout.
    val register: Externalizer[(TopologyContext) => Unit] = createRegistrationFunction(jobId, metrics)
    val spoutWithRegister = spout.openHook(register.get)

    formatOpt match {
      case Some(format) =>
        spoutWithRegister match {
          case schemeSpout: SchemeSpout[T] =>
            schemeSpout.getSpout[T](new FormattedScheme(_, format), schemeSpout.callOnOpen)
          case schemelessSpout =>
            schemelessSpout.getSpout match {
              case completable: CompletableSpout =>
                new CompletableFormattedSpout[T](completable, format)
              case nonCompletable =>
                new FormattedSpout[T](nonCompletable, format)
            }
        }
      case None => spoutWithRegister.getSpout
    }
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
