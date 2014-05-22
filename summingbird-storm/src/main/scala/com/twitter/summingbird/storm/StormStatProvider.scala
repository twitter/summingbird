package com.twitter.summingbird.storm

import backtype.storm.metric.api.CountMetric
import backtype.storm.task.TopologyContext
import com.twitter.summingbird.{ CounterIncrementor, PlatformStatProvider }
import com.twitter.summingbird.option.JobId
import org.slf4j.LoggerFactory

// Incrementor for Storm Counters 
// Returned to the Summingbird Counter object to call incrBy function in SB job code
private[summingbird] case class StormCounterIncrementor(metric: CountMetric) extends CounterIncrementor {
  def incrBy(by: Long): Unit = metric.incrBy(by)
}

private[summingbird] case class StormStatProvider(jobID: JobId,
                             context: TopologyContext,
                             metrics: List[(String, String)]) extends PlatformStatProvider {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormStatProvider])

  val stormMetrics: Map[String, CountMetric] = metrics.map {
    case (groupName, metricName) => (groupName + "/" + metricName, new CountMetric)
  }.toMap
  logger.debug("Stats for this Bolt: {}", stormMetrics.keySet mkString)

  def counterIncrementor(passedJobId: JobId, group: String, name: String): Option[StormCounterIncrementor] =
    if(passedJobId.get == jobID.get) {
        val metric = stormMetrics.getOrElse(group + "/" + name, sys.error("It is only valid to create counter objects during submission"))
        Some(StormCounterIncrementor(metric))
    } else {
      None
    }

  def registerMetrics: Unit =
    stormMetrics.foreach { case (name, metric) =>
      logger.debug("Registered metric {} with TopologyContext", name)
      context.registerMetric(name, metric, 60)
    }
}
