package com.twitter.summingbird.storm

import backtype.storm.metric.api.CountMetric
import backtype.storm.task.TopologyContext
import com.twitter.summingbird._
import org.slf4j.LoggerFactory

// Incrementor for Storm Counters 
// Returned to the Summingbird Counter object to call incrBy function in SB job code
case class StormCounterIncrementor(metric: CountMetric) extends CounterIncrementor {
  def incrBy(by: Long) = metric.incrBy(by) 
}

case class StormStatProvider(jobID: SummingbirdJobId,
                             context: TopologyContext,
                             metrics: List[(String, String)]) extends PlatformStatProvider {
  @transient private val logger = LoggerFactory.getLogger(classOf[StormStatProvider])

  val stormMetrics: Map[String, CountMetric] = metrics.map {
    case (groupName, metricName) => (groupName + "/" + metricName, new CountMetric)
  }.toMap
  logger.debug("Stats for this Bolt: {}", stormMetrics.keySet mkString)

  def counterIncrementor(passedJobId: SummingbirdJobId, group: String, name: String) =
    if(passedJobId.get == jobID.get) {
        val metric = stormMetrics.getOrElse(group + "/" + name, sys.error("It is only valid to create counter objects during submission"))
        //Some((by: Long) => metric.incrBy(by))
        Some(StormCounterIncrementor(metric))
    } else {
      None
    }

  def registerMetrics =
    stormMetrics.foreach { case (name, metric) =>
      logger.debug("In Bolt: registered metric {} with TopologyContext", name)
      context.registerMetric(name, metric, 60)
    }
}
