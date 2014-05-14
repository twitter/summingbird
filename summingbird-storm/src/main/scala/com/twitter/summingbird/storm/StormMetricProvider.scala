package com.twitter.summingbird.storm

import backtype.storm.metric.api.CountMetric
import backtype.storm.task.TopologyContext
import com.twitter.summingbird._
import org.slf4j.LoggerFactory

// Incrementor for Storm Stats
// Returned to the Summingbird Stats object to call incrBy function in SB job code
case class StormStatIncrementor(metric: CountMetric) extends StatIncrementor {
  def incrBy(by: Long) = metric.incrBy(by) 
}

case class StormMetricProvider(jobID: SummingbirdJobID,
                               context: TopologyContext,
                               metrics: List[(String, String)]) extends PlatformMetricProvider {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormMetricProvider])

  val stormMetrics: Map[String, CountMetric] = metrics.map {
    case (groupName, metricName) => (groupName + "/" + metricName, new CountMetric)
  }.toMap
  logger.debug("Metrics for this Bolt: {}", stormMetrics.keySet mkString)

  def incrementor(passedJobId: SummingbirdJobID, group: String, name: String) =
    if(passedJobId.get == jobID.get) {
        val metric = stormMetrics.getOrElse(group + "/" + name, sys.error("It is only valid to create stats objects during submission"))
        //Some((by: Long) => metric.incrBy(by))
        Some(StormStatIncrementor(metric))
    } else {
      None
    }

  def registerMetrics =
    stormMetrics.foreach { case (name, metric) =>
      logger.debug("In Bolt: registered metric {} with TopologyContext", name)
      context.registerMetric(name, metric, 60)
    }
}
